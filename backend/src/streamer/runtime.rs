use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use super::controller::StationController;
use super::pipeline::{PipelineError, PipelineEvent, StationPlaybackConfig};
use super::{SongInfo, StatusEvent};

enum StationCommand {
    Play(oneshot::Sender<Result<(), PipelineError>>),
    Pause(oneshot::Sender<Result<(), PipelineError>>),
    Shutdown(oneshot::Sender<Result<(), PipelineError>>),
    Skip(oneshot::Sender<Result<(), PipelineError>>),
    Reconnect(oneshot::Sender<Result<(), PipelineError>>),
    RetryReconnect {
        generation: u64,
        output_epoch: u64,
        attempt: u32,
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
    /// after the replace ran, so the controller can move to `Playing` on
    /// success or stay idle/retryable on failure.
    ResumeResult {
        result: Result<(), PipelineError>,
    },
}

struct ReconnectRetry {
    commands: mpsc::Sender<StationCommand>,
    generation: u64,
    output_epoch: u64,
    attempt: u32,
}

struct PendingPipelineAction {
    operation: super::driver::PipelineOperation,
    response: Option<oneshot::Sender<Result<(), PipelineError>>>,
    reconnect_retry: Option<ReconnectRetry>,
}

/// One unit of work for the sequential pipeline executor. `Shutdown` is a
/// barrier: everything buffered before it is discarded, the pipeline is
/// stopped, and the executor goes terminal — no operation can ever run after
/// the stop.
enum ExecutorTask {
    Operation(PendingPipelineAction),
    Shutdown {
        stop: super::driver::PipelineOperation,
        response: oneshot::Sender<Result<(), PipelineError>>,
    },
}

impl ExecutorTask {
    fn submit(self, operations: &mpsc::UnboundedSender<ExecutorTask>) {
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
            // Barrier: nothing buffered may run after the stop.
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
async fn run_executor(
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
    fn operation(operation: super::driver::PipelineOperation, response: Option<oneshot::Sender<Result<(), PipelineError>>>) -> Self {
        Self {
            operation,
            response,
            reconnect_retry: None,
        }
    }

    fn reconnect(
        target: super::pipeline::IcecastTarget,
        commands: mpsc::Sender<StationCommand>,
        generation: u64,
        output_epoch: u64,
        attempt: u32,
    ) -> Self {
        Self {
            operation: super::driver::PipelineOperation::Reconnect(target),
            response: None,
            reconnect_retry: Some(ReconnectRetry {
                commands,
                generation,
                output_epoch,
                attempt,
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
        let result = driver.execute(operation).await.map(|_| ());
        if let Some(response) = response {
            send(response, result);
            return;
        }
        if let Err(error) = result {
            if let Some(retry) = reconnect_retry {
                let delay = Duration::from_secs(1_u64 << retry.attempt.min(5));
                tracing::warn!(%error, generation = retry.generation, output_epoch = retry.output_epoch, ?delay, "retrying GStreamer output reconnect");
                // The backoff timer runs outside the pipeline executor:
                // sleeping here would block every other pipeline operation
                // (pause, skip, manual reconnect) for the whole backoff
                // window. The timer re-queues a single RetryReconnect; the
                // runtime rejects it via `reconnect_if_current` once the
                // generation/epoch is stale (successful reconnect, stop,
                // shutdown), so at most one retry chain is ever live.
                let commands = retry.commands;
                let generation = retry.generation;
                let output_epoch = retry.output_epoch;
                let attempt = retry.attempt.saturating_add(1);
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let _ = commands
                        .send(StationCommand::RetryReconnect {
                            generation,
                            output_epoch,
                            attempt,
                        })
                        .await;
                });
            } else {
                tracing::error!(error = %error, operation = %operation_description, "pipeline operation failed");
            }
        }
    }
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
                                    ExecutorTask::Operation(PendingPipelineAction::reconnect(target, retries.clone(), generation, output_epoch, 0))
                                        .submit(&operations_urgent);
                                }
                                Some(Ok(operation)) => ExecutorTask::Operation(PendingPipelineAction::operation(operation, None)).submit(&operations_urgent),
                                Some(Err(error)) => tracing::error!(error = %error, "failed to apply pipeline event"),
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
                        if let Some(operation) = controller.resume_from_idle().await {
                            let (completion, receiver) = tokio::sync::oneshot::channel();
                            ExecutorTask::Operation(PendingPipelineAction::operation(operation, Some(completion)))
                                .submit(&operations_regular);
                            let commands = retries.clone();
                            // The idle controller state is only advanced once
                            // the resume replace has actually succeeded (or
                            // kept retryable after a failure); the executor
                            // answers through the completion channel.
                            tokio::spawn(async move {
                                if let Ok(result) = receiver.await {
                                    let _ = commands.send(StationCommand::ResumeResult { result }).await;
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
                Ok(operation) => ExecutorTask::Operation(PendingPipelineAction::operation(operation, Some(response))).submit(operations),
                Err(error) => send(response, Err(error)),
            },
            Self::RetryReconnect {
                generation,
                output_epoch,
                attempt,
            } => match controller.reconnect_if_current(generation, output_epoch).await {
                Ok(Some(super::driver::PipelineOperation::Reconnect(target))) => {
                    ExecutorTask::Operation(PendingPipelineAction::reconnect(target, retries, generation, output_epoch, attempt))
                        .submit(operations);
                }
                Ok(Some(operation)) => ExecutorTask::Operation(PendingPipelineAction::operation(operation, None)).submit(operations),
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, generation, output_epoch, "failed to refresh Icecast reconnect target"),
            },
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
            Self::ResumeResult { result } => {
                controller.on_resume_result(result);
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;
    use crate::streamer::driver::{PipelineDriver, PipelineOperation};
    use crate::streamer::pipeline::{
        IcecastTarget, OutputConfig, PairPlan, PipelineSnapshot, PipelineState, PlaybackPipeline, RollingPlan,
    };

    struct CountingPipeline {
        replaces: AtomicUsize,
        stops: AtomicUsize,
        state_changes: AtomicUsize,
    }

    #[async_trait]
    impl PlaybackPipeline for CountingPipeline {
        async fn replace(&self, _: PairPlan) -> Result<(), PipelineError> {
            self.replaces.fetch_add(1, Ordering::Release);
            Ok(())
        }
        async fn roll(&self, _: RollingPlan) -> Result<(), PipelineError> {
            Ok(())
        }
        async fn apply_output(&self, _: OutputConfig) -> Result<(), PipelineError> {
            Ok(())
        }
        async fn set_playing(&self, _: bool) -> Result<(), PipelineError> {
            self.state_changes.fetch_add(1, Ordering::Release);
            Ok(())
        }
        async fn reconnect(&self, _: IcecastTarget) -> Result<(), PipelineError> {
            Ok(())
        }
        async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
            Ok(PipelineSnapshot {
                state: PipelineState::Stopped,
                elapsed: Duration::ZERO,
            })
        }
        async fn stop(&self) -> Result<(), PipelineError> {
            self.stops.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }

    fn set_playing_action(playing: bool) -> ExecutorTask {
        ExecutorTask::Operation(PendingPipelineAction::operation(PipelineOperation::SetPlaying(playing), None))
    }

    /// A closed lane must not drop operations still buffered on the other
    /// lane: the executor drains the remaining lane before exiting.
    #[tokio::test]
    async fn closing_the_urgent_lane_still_runs_regular_buffered_operations() {
        let (urgent_tx, urgent) = mpsc::unbounded_channel::<ExecutorTask>();
        let (regular_tx, regular) = mpsc::unbounded_channel::<ExecutorTask>();
        let pipeline = std::sync::Arc::new(CountingPipeline {
            replaces: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
        });
        let driver = PipelineDriver::spawn(pipeline.clone());

        // urgent: one op, then the sender goes away while regular still
        // buffers two operations.
        set_playing_action(false).submit(&urgent_tx);
        set_playing_action(false).submit(&regular_tx);
        set_playing_action(true).submit(&regular_tx);
        drop(urgent_tx);
        drop(regular_tx);

        run_executor(urgent, regular, driver).await;

        assert_eq!(pipeline.state_changes.load(Ordering::Acquire), 3);
    }

    /// The mirror image: regular closes while urgent still buffers work.
    #[tokio::test]
    async fn closing_the_regular_lane_still_runs_urgent_buffered_operations() {
        let (urgent_tx, urgent) = mpsc::unbounded_channel::<ExecutorTask>();
        let (regular_tx, regular) = mpsc::unbounded_channel::<ExecutorTask>();
        let pipeline = std::sync::Arc::new(CountingPipeline {
            replaces: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
        });
        let driver = PipelineDriver::spawn(pipeline.clone());

        set_playing_action(false).submit(&regular_tx);
        set_playing_action(false).submit(&urgent_tx);
        set_playing_action(true).submit(&urgent_tx);
        drop(regular_tx);
        drop(urgent_tx);

        run_executor(urgent, regular, driver).await;

        assert_eq!(pipeline.state_changes.load(Ordering::Acquire), 3);
    }

    /// The reconnect backoff timer must not occupy the pipeline executor:
    /// regular operations keep running while the retry is scheduled, and the
    /// retry is re-queued exactly once on the command channel.
    #[tokio::test]
    async fn reconnect_backoff_runs_outside_the_executor_and_requeues_a_single_retry() {
        struct FailingReconnectPipeline {
            reconnects: AtomicUsize,
            state_changes: AtomicUsize,
        }

        #[async_trait]
        impl PlaybackPipeline for FailingReconnectPipeline {
            async fn replace(&self, _: PairPlan) -> Result<(), PipelineError> {
                Ok(())
            }
            async fn roll(&self, _: RollingPlan) -> Result<(), PipelineError> {
                Ok(())
            }
            async fn apply_output(&self, _: OutputConfig) -> Result<(), PipelineError> {
                Ok(())
            }
            async fn set_playing(&self, _: bool) -> Result<(), PipelineError> {
                self.state_changes.fetch_add(1, Ordering::Release);
                Ok(())
            }
            async fn reconnect(&self, _: IcecastTarget) -> Result<(), PipelineError> {
                self.reconnects.fetch_add(1, Ordering::Release);
                Err(PipelineError::Pipeline("icecast unreachable".into()))
            }
            async fn snapshot(&self) -> Result<PipelineSnapshot, PipelineError> {
                Ok(PipelineSnapshot {
                    state: PipelineState::Stopped,
                    elapsed: Duration::ZERO,
                })
            }
            async fn stop(&self) -> Result<(), PipelineError> {
                Ok(())
            }
        }

        let (commands_tx, mut commands_rx) = mpsc::channel::<StationCommand>(32);
        let pipeline = std::sync::Arc::new(FailingReconnectPipeline {
            reconnects: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
        });
        let driver = PipelineDriver::spawn(pipeline.clone());
        let (urgent_tx, urgent) = mpsc::unbounded_channel::<ExecutorTask>();
        let (regular_tx, regular) = mpsc::unbounded_channel::<ExecutorTask>();
        let executor = tokio::spawn(run_executor(urgent, regular, driver));

        // A reconnect that fails, scheduling the backoff retry.
        ExecutorTask::Operation(PendingPipelineAction::reconnect(
            IcecastTarget::parse("localhost:8000", "secret".into(), "test", "test".into()).unwrap(),
            commands_tx.clone(),
            1,
            1,
            0,
        ))
        .submit(&urgent_tx);
        // While the backoff timer runs, the executor must still serve
        // regular work immediately.
        set_playing_action(false).submit(&regular_tx);

        // The single retry is re-queued on the command channel after the
        // backoff window (1 << 0 = 1s), with the next attempt number.
        let retry = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(StationCommand::RetryReconnect {
                    generation,
                    output_epoch,
                    attempt,
                }) = commands_rx.recv().await
                {
                    return (generation, output_epoch, attempt);
                }
            }
        })
        .await
        .expect("the reconnect retry must be re-queued after the backoff");
        assert_eq!(retry, (1, 1, 1));

        // The executor never slept: the regular operation ran during the
        // backoff, and exactly one reconnect attempt happened so far.
        assert_eq!(pipeline.state_changes.load(Ordering::Acquire), 1);
        assert_eq!(pipeline.reconnects.load(Ordering::Acquire), 1);

        drop(urgent_tx);
        drop(regular_tx);
        executor.await.unwrap();
    }

    /// A shutdown barrier on either lane discards everything buffered, stops
    /// the pipeline, answers the barrier caller, and ends the executor.
    #[tokio::test]
    async fn shutdown_barrier_stops_the_pipeline_and_discards_pending_work() {
        let (urgent_tx, urgent) = mpsc::unbounded_channel::<ExecutorTask>();
        let (regular_tx, regular) = mpsc::unbounded_channel::<ExecutorTask>();
        let pipeline = std::sync::Arc::new(CountingPipeline {
            replaces: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            state_changes: AtomicUsize::new(0),
        });
        let driver = PipelineDriver::spawn(pipeline.clone());

        // The barrier lands first, then more work lands on both lanes — it
        // must be discarded, never run after the stop.
        let (response, receiver) = oneshot::channel();
        ExecutorTask::Shutdown {
            stop: PipelineOperation::Stop,
            response,
        }
        .submit(&urgent_tx);
        set_playing_action(false).submit(&urgent_tx);
        set_playing_action(true).submit(&regular_tx);
        set_playing_action(true).submit(&regular_tx);
        drop(urgent_tx);
        drop(regular_tx);

        run_executor(urgent, regular, driver).await;

        assert!(receiver.await.unwrap().is_ok());
        // Nothing buffered before the barrier ran, nothing after it ran,
        // exactly one stop.
        assert_eq!(pipeline.state_changes.load(Ordering::Acquire), 0);
        assert_eq!(pipeline.stops.load(Ordering::Acquire), 1);
    }
}
