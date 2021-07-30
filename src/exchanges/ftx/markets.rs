use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Market {
    name: String,
}