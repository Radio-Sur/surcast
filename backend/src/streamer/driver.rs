use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use super::pipeline::{IcecastTarget, OutputConfig, PairPlan, PipelineError, PipelineSnapshot, PlaybackPipeline, RollingPlan};

#[derive(Debug)]
pub(crate) enum PipelineOperation {
    Replace(Box<PairPlan>),
    Roll(Box<RollingPlan>),
    ApplyOutput(OutputConfig),
    SetPlaying(bool),
    Stop,
    Reconnect(IcecastTarget),
    Snapshot,
}

pub(crate) enum PipelineOperationResult {
    Unit,
    Snapshot(PipelineSnapshot),
}
#[derive(Clone)]
pub(crate) struct PipelineDriver {
    commands: mpsc::UnboundedSender<DriverCommand>,
}

struct DriverCommand {
    operation: PipelineOperation,
    response: oneshot::Sender<Result<PipelineOperationResult, PipelineError>>,
}

impl PipelineDriver {
    pub(crate) fn spawn(pipeline: Arc<dyn PlaybackPipeline>) -> Self {
        let (commands, mut receiver) = mpsc::unbounded_channel::<DriverCommand>();
        tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                let result = execute(&*pipeline, command.operation).await;
                let _ = command.response.send(result);
            }
        });
        Self { commands }
    }

    pub(crate) async fn execute(&self, operation: PipelineOperation) -> Result<PipelineOperationResult, PipelineError> {
        let (response, receiver) = oneshot::channel();
        self.commands
            .send(DriverCommand { operation, response })
            .map_err(|_| PipelineError::Pipeline("pipeline driver stopped".into()))?;
        receiver
            .await
            .map_err(|_| PipelineError::Pipeline("pipeline driver stopped".into()))?
    }
}

async fn execute(pipeline: &dyn PlaybackPipeline, operation: PipelineOperation) -> Result<PipelineOperationResult, PipelineError> {
    match operation {
        PipelineOperation::Replace(plan) => {
            pipeline.replace(*plan).await?;
            Ok(PipelineOperationResult::Unit)
        }
        PipelineOperation::Roll(plan) => {
            pipeline.roll(*plan).await?;
            Ok(PipelineOperationResult::Unit)
        }
        PipelineOperation::ApplyOutput(output) => {
            pipeline.apply_output(output).await?;
            Ok(PipelineOperationResult::Unit)
        }
        PipelineOperation::SetPlaying(playing) => {
            pipeline.set_playing(playing).await?;
            Ok(PipelineOperationResult::Unit)
        }
        PipelineOperation::Stop => {
            pipeline.stop().await?;
            Ok(PipelineOperationResult::Unit)
        }
        PipelineOperation::Reconnect(target) => {
            pipeline.reconnect(target).await?;
            Ok(PipelineOperationResult::Unit)
        }
        PipelineOperation::Snapshot => Ok(PipelineOperationResult::Snapshot(pipeline.snapshot().await?)),
    }
}
