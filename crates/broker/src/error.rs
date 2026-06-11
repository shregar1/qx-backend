use thiserror::Error;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("topic: {0}")]
    Topic(#[from] qx_topic::TopicError),

    #[error("workflow: {0}")]
    Workflow(#[from] qx_workflow::WorkflowError),

    #[error("schema registry: {0}")]
    SchemaRegistry(#[from] qx_schema_registry::RegistryError),

    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),

    #[error("transport: {0}")]
    Transport(Box<tonic::transport::Error>),

    #[error("rpc: {0}")]
    Rpc(Box<tonic::Status>),

    #[error("topic not found: {0}")]
    TopicNotFound(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("{0}")]
    Custom(String),
}

impl From<tonic::transport::Error> for BrokerError {
    fn from(e: tonic::transport::Error) -> Self {
        Self::Transport(Box::new(e))
    }
}

impl From<tonic::Status> for BrokerError {
    fn from(e: tonic::Status) -> Self {
        Self::Rpc(Box::new(e))
    }
}

pub type Result<T> = std::result::Result<T, BrokerError>;
