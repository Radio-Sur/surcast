use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::controller::StationController;
use super::pipeline::{PipelineError, PipelineEvent, StationPlaybackConfig};
use super::{SongInfo, StatusEvent};

pub(crate) enum StationCommand {
    Play(oneshot::Sender<Result<(), PipelineError>>),
    Pause(oneshot::Sender<Result<(), PipelineError>>),
    Shutdown(oneshot::Sender<Result<(), PipelineError>>),
    Skip(oneshot::Sender<Result<(), PipelineError>>),
    Reconnect(oneshot::Sender<Result<(), PipelineError>>),
    RetryReconnect {
        generation: u64,
        output_epoch: u64,
        attempt: u32,
        token: u64,
    },
    /// The pipeline executor finished a reconnect chain: the controller
    /// ends its lifecycle (active token, output binding, shared completion)
    /// so a later disconnect for the same output starts a fresh chain. Sent
    /// after an automatic reconnect succeeded AND after a manual one-shot
    /// attempt finished either way — the chain is done, not necessarily the
    /// reconnect. `succeeded` tells the controller whether the reconnect
    /// actually succeeded, which is the only signal allowed to clear the
    /// known-disconnected marker.
    ReconnectFinished {
        token: u64,
        succeeded: bool,
    },
    Reload {
        songs: Vec<SongInfo>,
        align_next: bool,
        response: oneshot::Sender<Result<(), PipelineError>>,
    },
    UpdateConfig {
        config: StationPlaybackConfig,
        response: oneshot::Sender<Result<(), PipelineError>>,
    },
    PushQueueUpdate(oneshot::Sender<()>),
    TrimPlayedItems(oneshot::Sender<()>),
    Status(oneshot::Sender<Result<StatusEvent, PipelineError>>),
    /// Acknowledgment for an automatic idle resume: the executor answered
    /// after the replace ran. `attempt_id` correlates the outcome with the
    /// exact resume attempt; the controller ignores completions for
    /// attempts that a newer user decision (or a newer resume) superseded.
    ResumeResult {
        attempt_id: u64,
        result: Result<(), PipelineError>,
    },
}

pub(crate) struct ReconnectRetry {
    commands: mpsc::Sender<StationCommand>,
    generation: u64,
    output_epoch: u64,
    attempt: u32,
    /// Token of the reconnect retry chain that scheduled this retry; the
    /// runtime only proceeds while it still matches the active chain.
    token: u64,
    /// Shared view of the active chain token: the operation checks it right
    /// before touching the pipeline and skips itself when its chain was
    /// superseded (or invalidated by a stop) after being enqueued.
    token_shared: std::sync::Arc<crate::streamer::controller::ReconnectShared>,
    /// Whether the chain auto-retries after a pipeline failure. Automatic
    /// chains retry with exponential backoff; a manual reconnect is
    /// one-shot and its chain ends after the attempt either way.
    retry_on_failure: bool,
}

pub(crate) struct PendingPipelineAction {
    operation: super::driver::PipelineOperation,
    response: Option<oneshot::Sender<Result<(), PipelineError>>>,
    reconnect_retry: Option<ReconnectRetry>,
}

/// One unit of work for the sequential pipeline executor. `Shutdown` is a
/// barrier: everything buffered before it is discarded, the pipeline is
/// stopped, and the executor goes terminal — no operation can ever run after
/// the stop.
pub(crate) enum ExecutorTask {
    Operation(PendingPipelineAction),
    Shutdown {
        stop: super::driver::PipelineOperation,
        response: oneshot::Sender<Result<(), PipelineError>>,
    },
}

impl ExecutorTask {
    pub(crate) fn submit(self, operations: &mpsc::UnboundedSender<ExecutorTask>) {
        let _ = operations.send(self);
    }
}

/// Discards everything still buffered on one lane, answering the callers: a
/// pending operation is refused (it must not run after the stop), an extra
/// shutdown barrier is acknowledged as a no-op success (the first barrier
/// performs the stop).
fn discard_lane(lane: &mut mpsc::UnboundedReceiver<ExecutorTask>) {
    while let Ok(task) = lane.try_recv() {
        match task {
            ExecutorTask::Operation(action) => {
                if let Some(response) = action.response {
                    send(response, Err(PipelineError::Pipeline("station shutting down".to_owned())));
                }
            }
            ExecutorTask::Shutdown { response, .. } => {
                send(response, Ok(()));
            }
        }
    }
}

/// Runs one executor task; returns `false` once the executor reached its
/// terminal state (a shutdown barrier).
async fn run_task(
    task: ExecutorTask,
    driver: &super::driver::PipelineDriver,
    urgent: &mut mpsc::UnboundedReceiver<ExecutorTask>,
    regular: &mut mpsc::UnboundedReceiver<ExecutorTask>,
) -> bool {
    match task {
        ExecutorTask::Operation(action) => {
            action.run(driver.clone()).await;
            true
        }
        ExecutorTask::Shutdown { stop, response } => {
            discard_lane(urgent);
            discard_lane(regular);
            let result = driver.execute(stop).await.map(|_| ());
            send(response, result);
            false
        }
    }
}

/// Sequential pipeline executor with two priority lanes. Pipeline-driven
/// operations (handover attaches) always run ahead of command-driven ones
/// (reload realigns, replace restarts). A closed lane must not lose what the
/// other lane still buffers: on `None` the executor switches to draining the
/// remaining lane before exiting.
pub(crate) async fn run_executor(
    mut urgent: mpsc::UnboundedReceiver<ExecutorTask>,
    mut regular: mpsc::UnboundedReceiver<ExecutorTask>,
    driver: super::driver::PipelineDriver,
) {
    loop {
        tokio::select! {
            biased;
            task = urgent.recv() => {
                let Some(task) = task else {
                    while let Some(task) = regular.recv().await {
                        if !run_task(task, &driver, &mut urgent, &mut regular).await {
                            return;
                        }
                    }
                    return;
                };
                if !run_task(task, &driver, &mut urgent, &mut regular).await {
                    return;
                }
            }
            task = regular.recv() => {
                let Some(task) = task else {
                    while let Some(task) = urgent.recv().await {
                        if !run_task(task, &driver, &mut urgent, &mut regular).await {
                            return;
                        }
                    }
                    return;
                };
                if !run_task(task, &driver, &mut urgent, &mut regular).await {
                    return;
                }
            }
        }
    }
}

impl PendingPipelineAction {
    pub(crate) fn operation(
        operation: super::driver::PipelineOperation,
        response: Option<oneshot::Sender<Result<(), PipelineError>>>,
    ) -> Self {
        Self {
            operation,
            response,
            reconnect_retry: None,
        }
    }

    pub(crate) fn reconnect(
        target: super::pipeline::IcecastTarget,
        commands: mpsc::Sender<StationCommand>,
        generation: u64,
        output_epoch: u64,
        attempt: u32,
        token: u64,
        token_shared: std::sync::Arc<super::controller::ReconnectShared>,
        response: Option<oneshot::Sender<Result<(), PipelineError>>>,
        retry_on_failure: bool,
    ) -> Self {
        Self {
            operation: super::driver::PipelineOperation::Reconnect(target),
            response,
            reconnect_retry: Some(ReconnectRetry {
                commands,
                generation,
                output_epoch,
                attempt,
                token,
                token_shared,
                retry_on_failure,
            }),
        }
    }

    async fn run(self, driver: super::driver::PipelineDriver) {
        let Self {
            operation,
            response,
            reconnect_retry,
        } = self;
        let operation_description = format!("{operation:?}");
        // A queued reconnect checks its chain token right before touching
        // the pipeline: if the chain was superseded (a newer reconnect) or
        // invalidated (stop/shutdown) after this operation was enqueued,
        // the stale reconnect must never modify the pipeline — and a caller
        // waiting on a manual reconnect response gets a controlled error
        // instead of a dropped channel.
        if let Some(retry) = &reconnect_retry {
            if retry.token_shared.token() != retry.token {
                tracing::debug!(token = retry.token, "skipping stale queued reconnect; chain superseded");
                if let Some(response) = response {
                    send(
                        response,
                        Err(PipelineError::Pipeline("reconnect superseded before execution".to_owned())),
                    );
                }
                return;
            }
        }
        let result = driver.execute(operation).await.map(|_| ());
        // Ordering contract for reconnect-aware actions: the manual
        // response must never be delivered before the internal reconnect
        // lifecycle is finished. The caller's wake-up is the signal that
        // `mark_completed` ran and `ReconnectFinished` was enqueued (the
        // bounded command channel guarantees the enqueue happened before
        // the response, and the FIFO queue places the completion ahead of
        // any command the caller sends next). Without this ordering a
        // failed manual reconnect could wake the caller while the
        // controller still shows an active, uncompleted chain X — a
        // following Play would then refuse to start the recovery
        // (`resume_reconnect_for_break` sees "recovery already in
        // progress") and the output would stay disconnected with no future
        // work. Plain pipeline operations answer immediately (the `None`
        // arms); reconnect arms deliver the response after their lifecycle.
        match (result, reconnect_retry) {
            (Ok(()), None) => {
                if let Some(response) = response {
                    send(response, Ok(()));
                }
            }
            (Err(error), None) => {
                tracing::error!(error = %error, operation = %operation_description, "pipeline operation failed");
                if let Some(response) = response {
                    send(response, Err(error));
                }
            }
            (Ok(()), Some(retry)) => {
                // Mark the chain completed in the shared state BEFORE the
                // runtime processes ReconnectFinished: a disconnect landing
                // in this window must be treated as a fresh event, never
                // coalesced into the finished chain. The completion carries
                // the chain token, so an old in-flight reconnect can never
                // mark a newer chain as completed.
                retry.token_shared.mark_completed(retry.token);
                let _ = retry
                    .commands
                    .send(StationCommand::ReconnectFinished {
                        token: retry.token,
                        succeeded: true,
                    })
                    .await;
                if let Some(response) = response {
                    send(response, Ok(()));
                }
            }
            (Err(error), Some(retry)) if retry.retry_on_failure => {
                tracing::warn!(%error, generation = retry.generation, output_epoch = retry.output_epoch, token = retry.token, "retrying GStreamer output reconnect");
                // The backoff timer runs outside the pipeline executor:
                // sleeping here would block every other pipeline operation
                // (pause, skip, manual reconnect) for the whole backoff
                // window. The timer re-queues a single RetryReconnect with
                // the SAME chain token; the runtime drops it unless the
                // token still matches the active chain (a newer reconnect,
                // a stop, or a shutdown invalidates it) and
                // `reconnect_if_current` rejects it once the
                // generation/epoch is stale, so at most one retry chain is
                // ever live.
                schedule_reconnect_retry(retry.commands, retry.generation, retry.output_epoch, retry.attempt, retry.token);
                if let Some(response) = response {
                    send(response, Err(error));
                }
            }
            (Err(error), Some(retry)) => {
                // One-shot chain (manual reconnect): it ends here without a
                // retry timer; a future disconnect starts a fresh automatic
                // chain. The reconnect did NOT succeed: the known-disconnected
                // marker must survive so a later Play can recover.
                tracing::warn!(%error, token = retry.token, "manual reconnect failed; ending the chain");
                retry.token_shared.mark_completed(retry.token);
                let _ = retry
                    .commands
                    .send(StationCommand::ReconnectFinished {
                        token: retry.token,
                        succeeded: false,
                    })
                    .await;
                // The manual caller learns about the failure only after
                // the chain is completed and the completion event is
                // enqueued ahead of any command the caller may send next
                // (the command channel is FIFO).
                if let Some(response) = response {
                    send(response, Err(error));
                }
            }
        }
    }
}

/// Schedules the next attempt of a reconnect retry chain after the backoff
/// window (`1 << failed_attempt` seconds, capped). The timer runs outside
/// the pipeline executor and re-queues a single `RetryReconnect` carrying
/// the SAME chain token — a chain is identified by its token, not by its
/// attempts. A stale chain (superseded by a newer reconnect or invalidated
/// by a stop) is dropped by the runtime when the timer fires.
fn schedule_reconnect_retry(commands: mpsc::Sender<StationCommand>, generation: u64, output_epoch: u64, failed_attempt: u32, token: u64) {
    let delay = Duration::from_secs(1_u64 << failed_attempt.min(5));
    let attempt = failed_attempt.saturating_add(1);
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let _ = commands
            .send(StationCommand::RetryReconnect {
                generation,
                output_epoch,
                attempt,
                token,
            })
            .await;
    });
}

#[derive(Clone)]
pub(crate) struct StationRuntime {
    commands: mpsc::Sender<StationCommand>,
}

impl StationRuntime {
    pub(crate) fn spawn(mut controller: StationController, mut events: mpsc::UnboundedReceiver<PipelineEvent>) -> Self {
        let (commands, mut receiver) = mpsc::channel::<StationCommand>(32);
        let retries = commands.clone();
        // Pipeline operations run in a dedicated sequential executor, strictly
        // in submission order. Spawning one task per operation allowed a newer
        // plan (e.g. a skip/replace) to overtake an older queued roll, which
        // then failed as StalePlan after the graph had already been rebuilt.
        // Two priority lanes keep pipeline-driven operations (handover
        // attaches) ahead of command-driven ones (reload realigns, replace
        // restarts): the pipeline crossfades every ~1s, so an attach queued
        // behind a slow batch of realigns would always arrive after the
        // handover and stall the station.
        let (operations_urgent, urgent) = mpsc::unbounded_channel::<ExecutorTask>();
        let (operations_regular, regular) = mpsc::unbounded_channel::<ExecutorTask>();
        let executor_driver = controller.driver();
        tokio::spawn(run_executor(urgent, regular, executor_driver));
        // One per-station ticker wakes the runtime only while the controller
        // is idle (queue drained, waiting for AutoDJ / schedule fill), so a
        // future schedule entry starts playback without any API interaction.
        // The guard keeps the branch unregistered everywhere else: no wakeups
        // and no polling while playing, paused, or manually stopped.
        let mut idle_poll = tokio::time::interval(Duration::from_secs(1));
        idle_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    command = receiver.recv() => {
                        let Some(command) = command else { break };
                        if !command.run(&mut controller, retries.clone(), &operations_regular, &operations_urgent).await {
                            break;
                        }
                    },
                    event = events.recv() => match event {
                        Some(PipelineEvent::SinkDisconnected { generation, output_epoch, message }) => {
                            match controller.handle_event(PipelineEvent::SinkDisconnected { generation, output_epoch, message }).await {
                                Some(Ok(super::driver::PipelineOperation::Reconnect(target))) => {
                                    let token = controller.current_reconnect_token();
                                    let token_shared = controller.reconnect_token_shared();
                                    ExecutorTask::Operation(PendingPipelineAction::reconnect(
                                        target,
                                        retries.clone(),
                                        generation,
                                        output_epoch,
                                        0,
                                        token,
                                        token_shared,
                                        None,
                                        true,
                                    ))
                                    .submit(&operations_urgent);
                                }
                                Some(Ok(operation)) => ExecutorTask::Operation(PendingPipelineAction::operation(operation, None)).submit(&operations_urgent),
                                Some(Err(error)) => {
                                    // The chain just started (its token is
                                    // already active), but the target refresh
                                    // failed. A transient config/DB failure
                                    // must not kill the chain: schedule its
                                    // first retry.
                                    tracing::error!(%error, "failed to prepare reconnect after output drop; scheduling a retry");
                                    let token = controller.current_reconnect_token();
                                    schedule_reconnect_retry(retries.clone(), generation, output_epoch, 0, token);
                                }
                                None => {}
                            }
                        },
                        Some(event) => match controller.handle_event(event).await {
                            Some(Ok(operation)) => ExecutorTask::Operation(PendingPipelineAction::operation(operation, None)).submit(&operations_urgent),
                            Some(Err(error)) => tracing::error!(error = %error, "failed to apply pipeline event"),
                            None => {}
                        },
                        None => break,
                    },
                    _ = idle_poll.tick(), if controller.idle() => {
                        if let Some((operation, attempt_id)) = controller.resume_from_idle().await {
                            let (completion, receiver) = tokio::sync::oneshot::channel();
                            ExecutorTask::Operation(PendingPipelineAction::operation(operation, Some(completion)))
                                .submit(&operations_regular);
                            let commands = retries.clone();
                            // The idle controller state is only advanced once
                            // the resume replace has actually succeeded (or
                            // kept retryable after a failure); the executor
                            // answers through the completion channel. The
                            // attempt id lets the controller drop stale
                            // completions that a newer user decision
                            // superseded.
                            tokio::spawn(async move {
                                if let Ok(result) = receiver.await {
                                    let _ = commands.send(StationCommand::ResumeResult { attempt_id, result }).await;
                                }
                            });
                        }
                    },
                }
            }
        });
        Self { commands }
    }

    async fn request(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<(), PipelineError>>) -> StationCommand,
    ) -> Result<(), PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(command(response))
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?;
        receiver
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?
    }

    pub(crate) async fn play(&self) -> Result<(), PipelineError> {
        self.request(StationCommand::Play).await
    }

    pub(crate) async fn pause(&self) -> Result<(), PipelineError> {
        self.request(StationCommand::Pause).await
    }

    pub(crate) async fn shutdown(&self) -> Result<(), PipelineError> {
        self.request(StationCommand::Shutdown).await
    }

    pub(crate) async fn skip(&self) -> Result<(), PipelineError> {
        self.request(StationCommand::Skip).await
    }

    pub(crate) async fn reconnect(&self) -> Result<(), PipelineError> {
        self.request(StationCommand::Reconnect).await
    }

    pub(crate) async fn reload(&self, songs: Vec<SongInfo>, align_next: bool) -> Result<(), PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(StationCommand::Reload {
                songs,
                align_next,
                response,
            })
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?;
        receiver
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?
    }

    pub(crate) async fn update_config(&self, config: StationPlaybackConfig) -> Result<(), PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(StationCommand::UpdateConfig { config, response })
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?;
        receiver
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?
    }

    pub(crate) async fn push_queue_update(&self) -> Result<(), PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(StationCommand::PushQueueUpdate(response))
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?;
        receiver
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))
    }

    pub(crate) async fn trim_played_items(&self) -> Result<(), PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(StationCommand::TrimPlayedItems(response))
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?;
        receiver
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))
    }

    pub(crate) async fn status(&self) -> Result<StatusEvent, PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(StationCommand::Status(response))
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?;
        receiver
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?
    }
}

impl StationCommand {
    async fn run(
        self,
        controller: &mut StationController,
        retries: mpsc::Sender<StationCommand>,
        operations: &mpsc::UnboundedSender<ExecutorTask>,
        urgent: &mpsc::UnboundedSender<ExecutorTask>,
    ) -> bool {
        match self {
            Self::Play(response) => {
                ExecutorTask::Operation(PendingPipelineAction::operation(controller.play().await, Some(response))).submit(operations);
                // The pipeline does not restore a connection that broke
                // while paused: after the resume is queued, queue the
                // reconnect for the remembered break (full automatic chain:
                // token, output binding, retry on failure).
                if controller.is_output_known_disconnected() {
                    let generation = controller.generation();
                    let output_epoch = controller.output_epoch();
                    match controller.resume_reconnect_for_break().await {
                        Some(Ok(super::driver::PipelineOperation::Reconnect(target))) => {
                            let token = controller.current_reconnect_token();
                            let token_shared = controller.reconnect_token_shared();
                            ExecutorTask::Operation(PendingPipelineAction::reconnect(
                                target,
                                retries,
                                generation,
                                output_epoch,
                                0,
                                token,
                                token_shared,
                                None,
                                true,
                            ))
                            .submit(operations);
                        }
                        Some(Ok(operation)) => {
                            ExecutorTask::Operation(PendingPipelineAction::operation(operation, None)).submit(operations);
                        }
                        Some(Err(error)) => {
                            // Target refresh failed: retryable like a
                            // disconnect-time refresh failure.
                            let token = controller.current_reconnect_token();
                            tracing::warn!(%error, generation, output_epoch, "failed to prepare reconnect after pause; scheduling a retry");
                            schedule_reconnect_retry(retries, generation, output_epoch, 0, token);
                        }
                        None => {}
                    }
                }
            }
            Self::Pause(response) => {
                ExecutorTask::Operation(PendingPipelineAction::operation(controller.pause(), Some(response))).submit(operations);
            }
            Self::Shutdown(response) => {
                // The stop runs through the same sequential executor as every
                // other pipeline operation; the barrier goes to the urgent
                // lane so it preempts any pending regular work, discards
                // whatever is still buffered, and nothing can run after it.
                ExecutorTask::Shutdown {
                    stop: controller.stop(),
                    response,
                }
                .submit(urgent);
                return false;
            }
            Self::Skip(response) => match controller.skip().await {
                Ok(operation) => ExecutorTask::Operation(PendingPipelineAction::operation(operation, Some(response))).submit(operations),
                Err(error) => send(response, Err(error)),
            },
            Self::Reconnect(response) => match controller.reconnect().await {
                Ok(super::driver::PipelineOperation::Reconnect(target)) => {
                    // One-shot manual reconnect: the operation carries the
                    // chain token and shared state so a successful attempt
                    // ends the chain (ReconnectFinished) and a failed
                    // attempt also ends it without a retry timer — the
                    // controller never keeps an active token with no
                    // operation or timer behind it. The response reports the
                    // actual pipeline result to the caller.
                    let token = controller.current_reconnect_token();
                    let token_shared = controller.reconnect_token_shared();
                    controller.bind_reconnect_to_output(controller.generation(), controller.output_epoch());
                    ExecutorTask::Operation(PendingPipelineAction::reconnect(
                        target,
                        retries,
                        controller.generation(),
                        controller.output_epoch(),
                        0,
                        token,
                        token_shared,
                        Some(response),
                        false,
                    ))
                    .submit(operations);
                }
                Ok(operation) => ExecutorTask::Operation(PendingPipelineAction::operation(operation, Some(response))).submit(operations),
                Err(error) => send(response, Err(error)),
            },
            Self::RetryReconnect {
                generation,
                output_epoch,
                attempt,
                token,
            } => {
                // A stale chain (superseded by a newer reconnect or
                // invalidated by a stop) is dropped without touching the
                // pipeline; the token/epoch checks live in the controller.
                match controller.reconnect_if_current(generation, output_epoch, token).await {
                    Ok(Some(super::driver::PipelineOperation::Reconnect(target))) => {
                        let token_shared = controller.reconnect_token_shared();
                        ExecutorTask::Operation(PendingPipelineAction::reconnect(
                            target,
                            retries,
                            generation,
                            output_epoch,
                            attempt,
                            token,
                            token_shared,
                            None,
                            true,
                        ))
                        .submit(operations);
                    }
                    Ok(Some(operation)) => ExecutorTask::Operation(PendingPipelineAction::operation(operation, None)).submit(operations),
                    Ok(None) => {
                        // The chain is no longer eligible (its token was
                        // superseded, the output/generation went stale, or
                        // the station is no longer Playing): end it
                        // explicitly so no active token is ever left without
                        // future work. The token guard inside
                        // `end_reconnect_chain` protects a newer chain.
                        controller.end_reconnect_chain(token);
                    }
                    Err(error) => {
                        // A transient config/DB failure while refreshing the
                        // target is as retryable as a pipeline reconnect
                        // failure: schedule the next attempt of THIS chain
                        // (same token) after the backoff, outside the
                        // executor.
                        tracing::warn!(%error, generation, output_epoch, token, attempt, "failed to refresh Icecast reconnect target; retrying");
                        schedule_reconnect_retry(retries, generation, output_epoch, attempt, token);
                    }
                }
            }
            Self::ReconnectFinished { token, succeeded } => {
                if succeeded {
                    controller.on_reconnect_succeeded(token);
                } else {
                    controller.end_reconnect_chain(token);
                }
            }
            Self::Reload {
                songs,
                align_next,
                response,
            } => match controller.reload(songs, align_next).await {
                // The realignment roll runs in the sequential executor; a
                // lost race is non-fatal (the staged next simply keeps
                // playing), so a stale roll must not fail the API request.
                Ok(Some(operation)) => {
                    ExecutorTask::Operation(PendingPipelineAction::operation(operation, None)).submit(operations);
                    send(response, Ok(()));
                }
                Ok(None) => send(response, Ok(())),
                Err(error) => send(response, Err(error)),
            },
            Self::UpdateConfig { config, response } => match controller.update_config(config) {
                Some(operation) => {
                    ExecutorTask::Operation(PendingPipelineAction::operation(operation, Some(response))).submit(operations);
                }
                None => send(response, Ok(())),
            },
            Self::PushQueueUpdate(response) => {
                controller.push_queue_update().await;
                let _ = response.send(());
            }
            Self::TrimPlayedItems(response) => {
                controller.trim_played_items().await;
                let _ = response.send(());
            }
            Self::Status(response) => {
                let _ = response.send(controller.status().await);
            }
            Self::ResumeResult { attempt_id, result } => {
                controller.on_resume_result(attempt_id, result);
            }
        }
        true
    }
}

fn send(response: oneshot::Sender<Result<(), PipelineError>>, result: Result<(), PipelineError>) {
    let _ = response.send(result);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::streamer::driver::{PipelineDriver, PipelineOperation};
    use crate::streamer::testsupport::{self, Call, RecordingPipeline};

    /// Centralizes the executor scaffolding every test needs: the recording
    /// pipeline behind a driver, the urgent and regular operation lanes, the
    /// spawned executor task, and the command channel through which the
    /// executor reports `ReconnectFinished` / `RetryReconnect`.
    struct ExecutorHarness {
        pipeline: Arc<RecordingPipeline>,
        urgent_tx: mpsc::UnboundedSender<ExecutorTask>,
        regular_tx: mpsc::UnboundedSender<ExecutorTask>,
        commands: mpsc::Sender<StationCommand>,
        commands_rx: mpsc::Receiver<StationCommand>,
        executor: tokio::task::JoinHandle<()>,
    }

    impl ExecutorHarness {
        /// `commands_capacity` lets ordering tests use a bounded channel and
        /// pre-fill it with a dummy command.
        fn new(commands_capacity: usize) -> Self {
            Self::new_with(commands_capacity, Arc::new(RecordingPipeline::new()))
        }

        /// The pipeline is passed in so a test can install gates/failures
        /// before the driver is spawned around it.
        fn new_with(commands_capacity: usize, pipeline: Arc<RecordingPipeline>) -> Self {
            let (commands, commands_rx) = mpsc::channel::<StationCommand>(commands_capacity);
            let driver = PipelineDriver::spawn(pipeline.clone());
            let (urgent_tx, urgent) = mpsc::unbounded_channel::<ExecutorTask>();
            let (regular_tx, regular) = mpsc::unbounded_channel::<ExecutorTask>();
            let executor = tokio::spawn(run_executor(urgent, regular, driver));
            Self {
                pipeline,
                urgent_tx,
                regular_tx,
                commands,
                commands_rx,
                executor,
            }
        }

        fn submit_urgent(&self, task: ExecutorTask) {
            task.submit(&self.urgent_tx);
        }

        fn submit_regular(&self, task: ExecutorTask) {
            task.submit(&self.regular_tx);
        }

        fn set_playing_urgent(&self, playing: bool) {
            self.submit_urgent(set_playing_action(playing));
        }

        fn set_playing_regular(&self, playing: bool) {
            self.submit_regular(set_playing_action(playing));
        }

        /// Queues a shutdown barrier on the urgent lane and returns the
        /// barrier's response receiver.
        fn shutdown_barrier(&self) -> oneshot::Receiver<Result<(), PipelineError>> {
            let (response, receiver) = oneshot::channel();
            self.submit_urgent(ExecutorTask::Shutdown {
                stop: PipelineOperation::Stop,
                response,
            });
            receiver
        }

        /// Closes both lanes, waits for the executor to finish, and returns
        /// the pipeline and command receiver for post-mortem assertions.
        async fn finish(self) -> (Arc<RecordingPipeline>, mpsc::Receiver<StationCommand>) {
            drop(self.urgent_tx);
            drop(self.regular_tx);
            self.executor.await.unwrap();
            (self.pipeline, self.commands_rx)
        }
    }

    fn set_playing_action(playing: bool) -> ExecutorTask {
        ExecutorTask::Operation(PendingPipelineAction::operation(PipelineOperation::SetPlaying(playing), None))
    }

    /// Builds a reconnect action for chain `token` (generation 1 / epoch 1,
    /// attempt 0), returning the action and the shared token state so tests
    /// can supersede or probe the chain.
    fn reconnect_action(
        commands: mpsc::Sender<StationCommand>,
        token: u64,
        response: Option<oneshot::Sender<Result<(), PipelineError>>>,
        retry_on_failure: bool,
    ) -> (ExecutorTask, Arc<crate::streamer::controller::ReconnectShared>) {
        let shared = Arc::new(crate::streamer::controller::ReconnectShared::default());
        shared.set_token(token);
        (
            ExecutorTask::Operation(PendingPipelineAction::reconnect(
                testsupport::target(),
                commands,
                1,
                1,
                0,
                token,
                shared.clone(),
                response,
                retry_on_failure,
            )),
            shared,
        )
    }

    /// A closed lane must not drop operations still buffered on the other
    /// lane: the executor drains the remaining lane before exiting.
    #[tokio::test]
    async fn closing_the_urgent_lane_still_runs_regular_buffered_operations() {
        let harness = ExecutorHarness::new(32);

        harness.set_playing_urgent(false);
        harness.set_playing_regular(false);
        harness.set_playing_regular(true);
        let (pipeline, _) = harness.finish().await;

        assert_eq!(pipeline.count(Call::SetPlaying), 3);
    }

    /// The mirror image: regular closes while urgent still buffers work.
    #[tokio::test]
    async fn closing_the_regular_lane_still_runs_urgent_buffered_operations() {
        let harness = ExecutorHarness::new(32);

        harness.set_playing_regular(false);
        harness.set_playing_urgent(false);
        harness.set_playing_urgent(true);
        let (pipeline, _) = harness.finish().await;

        assert_eq!(pipeline.count(Call::SetPlaying), 3);
    }

    /// The reconnect backoff timer must not occupy the pipeline executor:
    /// regular operations keep running while the retry is scheduled, and the
    /// retry is re-queued exactly once on the command channel.
    #[tokio::test]
    async fn reconnect_backoff_runs_outside_the_executor_and_requeues_a_single_retry() {
        let mut harness = ExecutorHarness::new(32);
        harness.pipeline.fail(Call::Reconnect);

        let (action, _) = reconnect_action(harness.commands.clone(), 7, None, true);
        harness.submit_urgent(action);
        harness.set_playing_regular(false);

        let retry = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(StationCommand::RetryReconnect {
                    generation,
                    output_epoch,
                    attempt,
                    token,
                }) = harness.commands_rx.recv().await
                {
                    return (generation, output_epoch, attempt, token);
                }
            }
        })
        .await
        .expect("the reconnect retry must be re-queued after the backoff");
        assert_eq!(retry, (1, 1, 1, 7));

        assert_eq!(harness.pipeline.count(Call::SetPlaying), 1);
        assert_eq!(harness.pipeline.count(Call::Reconnect), 1);

        let _ = harness.finish().await;
    }

    /// A reconnect operation queued BEFORE its chain was superseded must
    /// not execute: the token check happens right before the pipeline call,
    /// so only the operation belonging to the current chain touches the
    /// pipeline.
    #[tokio::test]
    async fn superseded_queued_reconnect_is_skipped_before_the_pipeline() {
        let harness = ExecutorHarness::new(32);

        let (first, shared) = reconnect_action(harness.commands.clone(), 10, None, true);
        harness.submit_urgent(first);
        // Reconnect A (token 10) is queued, but superseded by token 11
        // before the executor runs it: A must be dropped by the
        // pre-pipeline token guard.
        shared.set_token(11);
        let (second, _) = reconnect_action(harness.commands.clone(), 11, None, true);
        harness.submit_urgent(second);
        let (pipeline, mut commands_rx) = harness.finish().await;

        assert_eq!(pipeline.count(Call::Reconnect), 1);
        let succeeded = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(StationCommand::ReconnectFinished { token, succeeded: true }) = commands_rx.recv().await {
                    return token;
                }
            }
        })
        .await
        .expect("a successful reconnect must report the finished chain");
        assert_eq!(succeeded, 11);
    }

    /// A queued reconnect must never run after a shutdown barrier discarded
    /// it: the executor stops the pipeline and nothing else touches it.
    #[tokio::test]
    async fn shutdown_discards_a_queued_reconnect() {
        let harness = ExecutorHarness::new(32);

        let (action, _) = reconnect_action(harness.commands.clone(), 10, None, true);
        harness.submit_regular(action);
        let receiver = harness.shutdown_barrier();
        let (pipeline, _) = harness.finish().await;

        assert!(receiver.await.unwrap().is_ok());
        assert_eq!(pipeline.count(Call::Reconnect), 0, "a discarded reconnect must never run");
        assert_eq!(pipeline.count(Call::Stop), 1);
    }

    /// A one-shot (manual) reconnect never schedules a retry: its chain ends
    /// after the failed attempt (ReconnectFinished) so the controller never
    /// keeps an active token without an operation or timer behind it, and a
    /// future disconnect starts a fresh automatic chain.
    #[tokio::test]
    async fn manual_one_shot_reconnect_ends_its_chain_after_failure_without_retrying() {
        let harness = ExecutorHarness::new(32);
        harness.pipeline.fail(Call::Reconnect);

        let (action, _) = reconnect_action(harness.commands.clone(), 5, None, false);
        harness.submit_urgent(action);
        // `finish` drops the harness's command sender; keep a clone alive so
        // the no-retry assertion below observes a live channel, not a closed
        // one (which would trivially satisfy the timeout).
        let _keep_alive = harness.commands.clone();
        let (pipeline, mut commands_rx) = harness.finish().await;

        let message = tokio::time::timeout(Duration::from_secs(1), commands_rx.recv())
            .await
            .expect("the one-shot chain must report its end")
            .expect("command channel must stay open");
        match message {
            StationCommand::ReconnectFinished { token, succeeded } => {
                assert_eq!(token, 5);
                assert!(!succeeded, "a failed one-shot reconnect must report no success");
            }
            _other => panic!("expected ReconnectFinished, got an unexpected command"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(300), commands_rx.recv()).await.is_err(),
            "a one-shot manual reconnect must not schedule a retry"
        );
        assert_eq!(pipeline.count(Call::Reconnect), 1);
    }

    /// Deterministic ordering contract for a FAILED manual reconnect: the
    /// response must not be delivered before the internal reconnect
    /// lifecycle is complete. A bounded command channel with capacity 1 is
    /// pre-filled with a dummy command, so the executor's
    /// `send(ReconnectFinished)` blocks; while it is blocked the manual
    /// caller must stay pending. Only after the channel is drained — and
    /// the completion is enqueued — may the response arrive.
    #[tokio::test]
    async fn manual_failure_response_waits_until_reconnect_finished_is_enqueued() {
        let mut harness = ExecutorHarness::new(1);
        harness.pipeline.fail(Call::Reconnect);

        let (dummy_tx, _dummy_rx) = oneshot::channel();
        harness.commands.send(StationCommand::PushQueueUpdate(dummy_tx)).await.unwrap();

        let (response, mut response_rx) = oneshot::channel();
        let (action, shared) = reconnect_action(harness.commands.clone(), 7, Some(response), false);
        harness.submit_urgent(action);
        // The executor finishes the pipeline reconnect and marks the chain
        // completed; its `send(ReconnectFinished)` then blocks on the full
        // channel. The manual response must still be pending — the caller
        // must never learn the result before the completion is enqueued.
        testsupport::wait_for("chain completion", || shared.is_current_completed()).await;
        assert!(
            response_rx.try_recv().is_err(),
            "the manual response must stay pending while ReconnectFinished cannot be enqueued"
        );

        // Drain the dummy: the completion is enqueued, and only then may
        // the manual caller observe the failure.
        let _dummy = harness.commands_rx.recv().await.expect("the dummy command is queued");
        let result = response_rx.await.expect("the manual caller must be answered");
        assert!(result.is_err(), "the manual caller must receive the real pipeline error");
        let finished = tokio::time::timeout(Duration::from_secs(2), harness.commands_rx.recv())
            .await
            .expect("ReconnectFinished must arrive")
            .expect("command channel must stay open");
        match finished {
            StationCommand::ReconnectFinished { token, succeeded } => {
                assert_eq!(token, 7);
                assert!(!succeeded, "a failed manual reconnect must report no success");
            }
            _other => panic!("expected ReconnectFinished, got an unexpected command"),
        }

        let (pipeline, _) = harness.finish().await;
        assert_eq!(pipeline.count(Call::Reconnect), 1);
    }

    /// The same ordering contract for a SUCCESSFUL manual reconnect: the
    /// completion event is enqueued (and the chain marked completed) before
    /// the caller sees Ok, so a following Play can never run a redundant
    /// recovery.
    #[tokio::test]
    async fn manual_success_response_waits_until_reconnect_finished_is_enqueued() {
        let mut harness = ExecutorHarness::new(1);

        let (dummy_tx, _dummy_rx) = oneshot::channel();
        harness.commands.send(StationCommand::PushQueueUpdate(dummy_tx)).await.unwrap();

        let (response, mut response_rx) = oneshot::channel();
        let (action, shared) = reconnect_action(harness.commands.clone(), 8, Some(response), false);
        harness.submit_urgent(action);

        // The chain is completed, but `send(ReconnectFinished)` is blocked
        // on the full channel: the manual response must stay pending.
        testsupport::wait_for("chain completion", || shared.is_current_completed()).await;
        assert!(
            response_rx.try_recv().is_err(),
            "the manual response must stay pending while ReconnectFinished cannot be enqueued"
        );

        let _dummy = harness.commands_rx.recv().await.expect("the dummy command is queued");
        let result = response_rx.await.expect("the manual caller must be answered");
        assert!(result.is_ok(), "the manual caller must receive Ok on success");
        let finished = tokio::time::timeout(Duration::from_secs(2), harness.commands_rx.recv())
            .await
            .expect("ReconnectFinished must arrive")
            .expect("command channel must stay open");
        match finished {
            StationCommand::ReconnectFinished { token, succeeded } => {
                assert_eq!(token, 8);
                assert!(succeeded, "a successful manual reconnect must report success");
            }
            _other => panic!("expected ReconnectFinished, got an unexpected command"),
        }

        let (pipeline, _) = harness.finish().await;
        assert_eq!(pipeline.count(Call::Reconnect), 1);
    }

    #[tokio::test]
    async fn manual_reconnect_returns_the_pipeline_result_on_success() {
        let harness = ExecutorHarness::new(32);

        let (response, receiver) = oneshot::channel();
        let (action, _) = reconnect_action(harness.commands.clone(), 5, Some(response), false);
        harness.submit_urgent(action);
        let (pipeline, _) = harness.finish().await;

        assert_eq!(pipeline.count(Call::Reconnect), 1);
        assert!(
            receiver.await.unwrap().is_ok(),
            "the manual caller must receive the pipeline result"
        );
    }

    /// A failed manual reconnect delivers the actual PipelineError — not a
    /// cancelled channel — runs exactly once, and stays one-shot (no retry
    /// timer).
    #[tokio::test]
    async fn manual_reconnect_returns_the_pipeline_error_on_failure() {
        let harness = ExecutorHarness::new(32);
        harness.pipeline.fail(Call::Reconnect);

        let (response, receiver) = oneshot::channel();
        let (action, _) = reconnect_action(harness.commands.clone(), 5, Some(response), false);
        harness.submit_urgent(action);
        // Keep the command channel open after `finish` so the no-retry
        // assertion below observes a live channel, not a closed one.
        let _keep_alive = harness.commands.clone();
        let (pipeline, mut commands_rx) = harness.finish().await;

        let result = receiver.await.expect("the manual caller must not get a cancelled channel");
        match result {
            Err(PipelineError::Pipeline(message)) => assert!(message.contains("injected failure"), "unexpected error: {message}"),
            other => panic!("expected the pipeline error, got {other:?}"),
        }
        assert_eq!(pipeline.count(Call::Reconnect), 1, "the manual reconnect must run exactly once");
        let message = tokio::time::timeout(Duration::from_secs(1), commands_rx.recv())
            .await
            .expect("the one-shot chain must report its end")
            .expect("command channel must stay open");
        match message {
            StationCommand::ReconnectFinished { token, succeeded } => {
                assert_eq!(token, 5);
                assert!(!succeeded, "a failed one-shot reconnect must report no success");
            }
            _other => panic!("expected ReconnectFinished, got an unexpected command"),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(300), commands_rx.recv()).await.is_err(),
            "a one-shot manual reconnect must not schedule a retry"
        );
    }

    /// A manual reconnect superseded before the pipeline ran still answers
    /// its caller with a controlled error instead of a dropped channel.
    #[tokio::test]
    async fn superseded_manual_reconnect_answers_its_caller_with_an_error() {
        let harness = ExecutorHarness::new(32);

        let (response, receiver) = oneshot::channel();
        let (action, shared) = reconnect_action(harness.commands.clone(), 10, Some(response), false);
        harness.submit_urgent(action);
        // The manual reconnect (token 10) is queued, then superseded by
        // token 11 before the executor runs it.
        shared.set_token(11);
        let (pipeline, _) = harness.finish().await;

        assert_eq!(
            pipeline.count(Call::Reconnect),
            0,
            "the stale reconnect must not touch the pipeline"
        );
        let result = receiver
            .await
            .expect("the superseded caller must get a controlled answer, not a dropped channel");
        assert!(result.is_err(), "the superseded manual reconnect must report an error");
    }

    /// A stale in-flight reconnect finishing late must not mark a NEWER
    /// chain as completed: completion is correlated with the token.
    #[tokio::test]
    async fn stale_in_flight_completion_cannot_mark_a_newer_chain_completed() {
        let pipeline = Arc::new(RecordingPipeline::with_gates());
        let harness = ExecutorHarness::new_with(32, pipeline.clone());

        let (action, shared) = reconnect_action(harness.commands.clone(), 10, None, true);
        harness.submit_urgent(action);
        let gate = pipeline.reconnect_gate.as_ref().expect("gated pipeline");
        tokio::time::timeout(Duration::from_secs(2), gate.wait_started())
            .await
            .expect("reconnect X must reach the pipeline");

        // X is blocked inside the pipeline; Y becomes the current chain
        // before X finishes, so X completing late must not mark Y as
        // completed.
        shared.set_token(11);

        gate.release();
        let (pipeline, _) = harness.finish().await;

        assert_eq!(pipeline.count(Call::Reconnect), 1);
        assert_eq!(shared.token(), 11, "the current chain is still Y");
        assert!(
            !shared.is_current_completed(),
            "a stale in-flight completion (token 10) must not mark chain Y (token 11) as completed"
        );
    }

    /// A shutdown barrier on either lane discards everything buffered, stops
    /// the pipeline, answers the barrier caller, and ends the executor.
    #[tokio::test]
    async fn shutdown_barrier_stops_the_pipeline_and_discards_pending_work() {
        let harness = ExecutorHarness::new(32);

        let receiver = harness.shutdown_barrier();
        harness.set_playing_urgent(false);
        harness.set_playing_regular(true);
        harness.set_playing_regular(true);
        let (pipeline, _) = harness.finish().await;

        assert!(receiver.await.unwrap().is_ok());
        assert_eq!(pipeline.count(Call::SetPlaying), 0);
        assert_eq!(pipeline.count(Call::Stop), 1);
    }
}
