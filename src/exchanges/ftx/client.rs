use reqwest::Client;

pub struct RestClient {
    pub header: &'static str,
    pub endpoint: &'static str,
    pub client: Client,
}

impl RestClient {
    pub const INTL_ENDPOINT: &'static str = "https://ftx.com/api";
    pub const US_ENDPOINT: &'static str = "https://ftx.us/api";
    pub const INTL_HEADER: &'static str = "FTX";
    pub const US_HEADER: &'static str = "FTXUS";

    pub fn new(endpoint: &'static str, header: &'static str) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();
        Self {
            header,
            endpoint,
            client,
        }
    }

    pub fn new_intl() -> Self {
        Self::new(Self::INTL_ENDPOINT, Self::INTL_HEADER)
    }

    pub fn new_us() -> Self {
        Self::new(Self::US_ENDPOINT, Self::US_HEADER)
    }

    pub fn get() {}

    pub fn post() {}

    pub fn request() {}
}
