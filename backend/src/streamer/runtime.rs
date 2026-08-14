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

    fn submit(self, operations: &mpsc::UnboundedSender<PendingPipelineAction>) {
        let _ = operations.send(self);
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
                tokio::time::sleep(delay).await;
                let _ = retry
                    .commands
                    .send(StationCommand::RetryReconnect {
                        generation: retry.generation,
                        output_epoch: retry.output_epoch,
                        attempt: retry.attempt.saturating_add(1),
                    })
                    .await;
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
        let (operations_urgent, mut urgent) = mpsc::unbounded_channel::<PendingPipelineAction>();
        let (operations_regular, mut regular) = mpsc::unbounded_channel::<PendingPipelineAction>();
        let executor_driver = controller.driver();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    action = urgent.recv() => {
                        let Some(action) = action else { return };
                        action.run(executor_driver.clone()).await;
                    }
                    action = regular.recv() => {
                        let Some(action) = action else { return };
                        action.run(executor_driver.clone()).await;
                    }
                }
            }
        });
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
                        if !command.run(&mut controller, retries.clone(), &operations_regular).await {
                            break;
                        }
                    },
                    event = events.recv() => match event {
                        Some(PipelineEvent::SinkDisconnected { generation, output_epoch, message }) => {
                            match controller.handle_event(PipelineEvent::SinkDisconnected { generation, output_epoch, message }).await {
                                Some(Ok(super::driver::PipelineOperation::Reconnect(target))) => {
                                    PendingPipelineAction::reconnect(target, retries.clone(), generation, output_epoch, 0)
                                        .submit(&operations_urgent);
                                }
                                Some(Ok(operation)) => PendingPipelineAction::operation(operation, None).submit(&operations_urgent),
                                Some(Err(error)) => tracing::error!(error = %error, "failed to apply pipeline event"),
                                None => {}
                            }
                        },
                        Some(event) => match controller.handle_event(event).await {
                            Some(Ok(operation)) => PendingPipelineAction::operation(operation, None).submit(&operations_urgent),
                            Some(Err(error)) => tracing::error!(error = %error, "failed to apply pipeline event"),
                            None => {}
                        },
                        None => break,
                    },
                    _ = idle_poll.tick(), if controller.idle() => {
                        if let Some(operation) = controller.resume_from_idle().await {
                            PendingPipelineAction::operation(operation, None).submit(&operations_regular);
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
        operations: &mpsc::UnboundedSender<PendingPipelineAction>,
    ) -> bool {
        match self {
            Self::Play(response) => PendingPipelineAction::operation(controller.play().await, Some(response)).submit(operations),
            Self::Pause(response) => PendingPipelineAction::operation(controller.pause(), Some(response)).submit(operations),
            Self::Shutdown(response) => {
                let result = controller.driver().execute(controller.stop()).await.map(|_| ());
                send(response, result);
                return false;
            }
            Self::Skip(response) => match controller.skip().await {
                Ok(operation) => PendingPipelineAction::operation(operation, Some(response)).submit(operations),
                Err(error) => send(response, Err(error)),
            },
            Self::Reconnect(response) => match controller.reconnect().await {
                Ok(operation) => PendingPipelineAction::operation(operation, Some(response)).submit(operations),
                Err(error) => send(response, Err(error)),
            },
            Self::RetryReconnect {
                generation,
                output_epoch,
                attempt,
            } => match controller.reconnect_if_current(generation, output_epoch).await {
                Ok(Some(super::driver::PipelineOperation::Reconnect(target))) => {
                    PendingPipelineAction::reconnect(target, retries, generation, output_epoch, attempt).submit(operations);
                }
                Ok(Some(operation)) => PendingPipelineAction::operation(operation, None).submit(operations),
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
                    PendingPipelineAction::operation(operation, None).submit(operations);
                    send(response, Ok(()));
                }
                Ok(None) => send(response, Ok(())),
                Err(error) => send(response, Err(error)),
            },
            Self::UpdateConfig { config, response } => match controller.update_config(config) {
                Some(operation) => PendingPipelineAction::operation(operation, Some(response)).submit(operations),
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
        }
        true
    }
}

fn send(response: oneshot::Sender<Result<(), PipelineError>>, result: Result<(), PipelineError>) {
    let _ = response.send(result);
}
