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
    Reload {
        songs: Vec<SongInfo>,
        response: oneshot::Sender<Result<(), PipelineError>>,
    },
    UpdateConfig {
        config: StationPlaybackConfig,
        response: oneshot::Sender<Result<(), PipelineError>>,
    },
    PushQueueUpdate(oneshot::Sender<()>),
    TrimPlayedItems(oneshot::Sender<()>),
    Status(oneshot::Sender<StatusEvent>),
}

#[derive(Clone)]
pub(crate) struct StationRuntime {
    commands: mpsc::Sender<StationCommand>,
}

impl StationRuntime {
    pub(crate) fn spawn(mut controller: StationController, mut events: mpsc::UnboundedReceiver<PipelineEvent>) -> Self {
        let (commands, mut receiver) = mpsc::channel::<StationCommand>(32);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    command = receiver.recv() => {
                        let Some(command) = command else { break };
                        if !command.run(&mut controller).await {
                            break;
                        }
                    },
                    event = events.recv() => match event {
                        Some(event) => match controller.handle_event(event).await {
                            Some(Ok(operation)) => launch(controller.driver(), operation, None),
                            Some(Err(error)) => tracing::error!(error = %error, "failed to apply pipeline event"),
                            None => {}
                        },
                        None => break,
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

    pub(crate) async fn reload(&self, songs: Vec<SongInfo>) -> Result<(), PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(StationCommand::Reload { songs, response })
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

    pub(crate) async fn push_queue_update(&self) {
        let (response, receiver) = oneshot::channel();
        if self.commands.send(StationCommand::PushQueueUpdate(response)).await.is_ok() {
            let _ = receiver.await;
        }
    }

    pub(crate) async fn trim_played_items(&self) {
        let (response, receiver) = oneshot::channel();
        if self.commands.send(StationCommand::TrimPlayedItems(response)).await.is_ok() {
            let _ = receiver.await;
        }
    }

    pub(crate) async fn status(&self) -> Result<StatusEvent, PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(StationCommand::Status(response))
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))?;
        receiver
            .await
            .map_err(|_| PipelineError::Pipeline("station runtime stopped".to_owned()))
    }
}

impl StationCommand {
    async fn run(self, controller: &mut StationController) -> bool {
        match self {
            Self::Play(response) => launch(controller.driver(), controller.play(), Some(response)),
            Self::Pause(response) => launch(controller.driver(), controller.pause(), Some(response)),
            Self::Shutdown(response) => {
                let result = controller.driver().execute(controller.stop()).await.map(|_| ());
                send(response, result);
                return false;
            }
            Self::Skip(response) => match controller.skip().await {
                Ok(operation) => launch(controller.driver(), operation, Some(response)),
                Err(error) => send(response, Err(error)),
            },
            Self::Reconnect(response) => launch(controller.driver(), controller.reconnect(), Some(response)),
            Self::Reload { songs, response } => send(response, controller.reload(songs).await),
            Self::UpdateConfig { config, response } => match controller.update_config(config) {
                Some(operation) => launch(controller.driver(), operation, Some(response)),
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

fn launch(
    driver: super::driver::PipelineDriver,
    operation: super::driver::PipelineOperation,
    response: Option<oneshot::Sender<Result<(), PipelineError>>>,
) {
    tokio::spawn(async move {
        let result = driver.execute(operation).await.map(|_| ());
        if let Some(response) = response {
            send(response, result);
        } else if let Err(error) = result {
            tracing::error!(error = %error, "pipeline operation failed");
        }
    });
}

fn send(response: oneshot::Sender<Result<(), PipelineError>>, result: Result<(), PipelineError>) {
    let _ = response.send(result);
}
