use thiserror::Error;
use super::Channel;
use tokio_tungstenite::tungstenite;

#[derive(Debug, Error)]
pub enum RestError {
    #[error("Api error: {0}")]
    Api(String),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Error)]
pub enum WSError {
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