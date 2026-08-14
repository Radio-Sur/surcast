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

enum PendingPipelineAction {
    Execute {
        operation: super::driver::PipelineOperation,
        response: Option<oneshot::Sender<Result<(), PipelineError>>>,
        reconnect_retry: Option<ReconnectRetry>,
    },
}

impl PendingPipelineAction {
    fn operation(operation: super::driver::PipelineOperation, response: Option<oneshot::Sender<Result<(), PipelineError>>>) -> Self {
        Self::Execute {
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
        Self::Execute {
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

    fn launch(self, driver: super::driver::PipelineDriver) {
        tokio::spawn(async move {
            let Self::Execute {
                operation,
                response,
                reconnect_retry,
            } = self;
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
                    tracing::error!(error = %error, "pipeline operation failed");
                }
            }
        });
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
                        if !command.run(&mut controller, retries.clone()).await {
                            break;
                        }
                    },
                    event = events.recv() => match event {
                        Some(PipelineEvent::SinkDisconnected { generation, output_epoch, message }) => {
                            match controller.handle_event(PipelineEvent::SinkDisconnected { generation, output_epoch, message }).await {
                                Some(Ok(super::driver::PipelineOperation::Reconnect(target))) => {
                                    PendingPipelineAction::reconnect(target, retries.clone(), generation, output_epoch, 0)
                                        .launch(controller.driver());
                                }
                                Some(Ok(operation)) => PendingPipelineAction::operation(operation, None).launch(controller.driver()),
                                Some(Err(error)) => tracing::error!(error = %error, "failed to apply pipeline event"),
                                None => {}
                            }
                        },
                        Some(event) => match controller.handle_event(event).await {
                            Some(Ok(operation)) => PendingPipelineAction::operation(operation, None).launch(controller.driver()),
                            Some(Err(error)) => tracing::error!(error = %error, "failed to apply pipeline event"),
                            None => {}
                        },
                        None => break,
                    },
                    _ = idle_poll.tick(), if controller.idle() => {
                        if let Some(operation) = controller.resume_from_idle().await {
                            PendingPipelineAction::operation(operation, None).launch(controller.driver());
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
    async fn run(self, controller: &mut StationController, retries: mpsc::Sender<StationCommand>) -> bool {
        match self {
            Self::Play(response) => PendingPipelineAction::operation(controller.play().await, Some(response)).launch(controller.driver()),
            Self::Pause(response) => PendingPipelineAction::operation(controller.pause(), Some(response)).launch(controller.driver()),
            Self::Shutdown(response) => {
                let result = controller.driver().execute(controller.stop()).await.map(|_| ());
                send(response, result);
                return false;
            }
            Self::Skip(response) => match controller.skip().await {
                Ok(operation) => PendingPipelineAction::operation(operation, Some(response)).launch(controller.driver()),
                Err(error) => send(response, Err(error)),
            },
            Self::Reconnect(response) => match controller.reconnect().await {
                Ok(operation) => PendingPipelineAction::operation(operation, Some(response)).launch(controller.driver()),
                Err(error) => send(response, Err(error)),
            },
            Self::RetryReconnect {
                generation,
                output_epoch,
                attempt,
            } => match controller.reconnect_if_current(generation, output_epoch).await {
                Ok(Some(super::driver::PipelineOperation::Reconnect(target))) => {
                    PendingPipelineAction::reconnect(target, retries, generation, output_epoch, attempt).launch(controller.driver());
                }
                Ok(Some(operation)) => PendingPipelineAction::operation(operation, None).launch(controller.driver()),
                Ok(None) => {}
                Err(error) => tracing::warn!(%error, generation, output_epoch, "failed to refresh Icecast reconnect target"),
            },
            Self::Reload {
                songs,
                align_next,
                response,
            } => match controller.reload(songs, align_next).await {
                Ok(Some(operation)) => {
                    // Executed synchronously so a stale handover event cannot
                    // interleave with the swap; a lost race is non-fatal (the
                    // staged next simply keeps playing).
                    if let Err(error) = controller.driver().execute(operation).await {
                        tracing::warn!(%error, "queue realignment roll failed; keeping the staged next");
                    }
                    send(response, Ok(()));
                }
                Ok(None) => send(response, Ok(())),
                Err(error) => send(response, Err(error)),
            },
            Self::UpdateConfig { config, response } => match controller.update_config(config) {
                Some(operation) => PendingPipelineAction::operation(operation, Some(response)).launch(controller.driver()),
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
