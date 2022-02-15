use super::Channel;
use thiserror::Error;
use tokio_tungstenite::tungstenite;

#[derive(Debug, Error)]
pub enum WsError {
    #[error("Not subscribed to this channel {0:?}")]
    NotSubscribedToThisChannel(Channel),
    #[error("Missing subscription confirmation")]
    MissingSubscriptionConfirmation,
    #[error("Socket is not authenticated")]
    SocketNotAuthenticated,
    #[error(transparent)]
    Tungstenite(#[from] tungstenite::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}
