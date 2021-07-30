use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Market {
    name: String,
}