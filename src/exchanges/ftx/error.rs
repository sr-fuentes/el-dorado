#[derive(Debug)]
pub enum RestError {
    Reqwest(reqwest::Error),
    Api(String)
}

impl From<reqwest::Error> for RestError {
    fn from(e: reqwest::Error) -> RestError {
        RestError::Reqwest(e)
    }
}

