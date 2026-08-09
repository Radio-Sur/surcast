use tokio::sync::{mpsc, oneshot};

use super::controller::StationController;
use super::pipeline::{PipelineError, PipelineEvent, StationPlaybackConfig};
use super::{SongInfo, StatusEvent};

enum StationCommand {
    Play(oneshot::Sender<Result<(), PipelineError>>),
    Pause(oneshot::Sender<Result<(), PipelineError>>),
    Stop(oneshot::Sender<Result<(), PipelineError>>),
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
                    Some(command) = receiver.recv() => command.run(&mut controller).await,
                    Some(event) = events.recv() => controller.handle_event(event).await,
                    else => break,
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

    pub(crate) async fn stop(&self) -> Result<(), PipelineError> {
        self.request(StationCommand::Stop).await
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
    async fn run(self, controller: &mut StationController) {
        match self {
            Self::Play(response) => send(response, controller.play().await),
            Self::Pause(response) => send(response, controller.pause().await),
            Self::Stop(response) => send(response, controller.stop().await),
            Self::Skip(response) => send(response, controller.skip().await),
            Self::Reconnect(response) => send(response, controller.reconnect().await),
            Self::Reload { songs, response } => send(response, controller.reload(songs).await),
            Self::UpdateConfig { config, response } => {
                controller.update_config(config);
                send(response, Ok(()));
            }
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
    }
}

fn send(response: oneshot::Sender<Result<(), PipelineError>>, result: Result<(), PipelineError>) {
    let _ = response.send(result);
}
