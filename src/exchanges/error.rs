use crate::exchanges::ws::Channel;
use thiserror::Error;
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
pub enum WsError {
    #[error("Not subscribed to this channel {0:?}")]
    NotSubscribedToThisChannel(Channel),
    #[error("Missing subscription confirmation")]
    MissingSubscriptionConfirmation,
    #[error("Socket is not authenticated")]
    SocketNotAuthenticated,
    #[error("Too much time has elapsed since last message")]
    TimeSinceLastMsg,
    #[error(transparent)]
    Tungstenite(#[from] tungstenite::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}
