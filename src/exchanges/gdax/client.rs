use crate::exchanges::error::RestError;
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde_json::{from_reader, Map, Value};

#[derive(Debug)]
pub struct RestClient {
    pub endpoint: &'static str,
    pub client: Client,
}

impl RestClient {
    pub const ENDPOINT: &'static str = "https://api.exchange.coinbase.com";

    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .user_agent("ALMEJAL")
            .build()
            .unwrap();
        Self {
            endpoint: Self::ENDPOINT,
            client,
        }
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        params: Option<Value>,
    ) -> Result<T, RestError> {
        self.request(Method::GET, path, params).await
    }

    pub async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        params: Option<Value>,
    ) -> Result<T, RestError> {
        let params = params.map(|value| {
            if let Value::Object(map) = value {
                map.into_iter()
                    .filter(|(_, v)| v != &Value::Null)
                    .collect::<Map<String, Value>>()
            } else {
                panic!("Invalid params.");
            }
        });

        println!("Params: {:?}", params);

        let response = self
            .client
            .request(method, format!("{}{}", self.endpoint, path))
            .query(&params)
            .header("ContentType", "application/json")
            .send()
            .await?;

        println!("Response: {:?}", response);
        match response.error_for_status() {
            Ok(res) => {
                let res_bytes = res.bytes().await?;
                match from_reader(&*res_bytes) {
                    Ok(result) => Ok(result),
                    Err(e) => {
                        println!(
                            "Reqwest resp: {:?}",
                            std::str::from_utf8(&res_bytes).unwrap()
                        );
                        println!("Error: {:?}", e.to_string());
                        eprintln!("Errorpl: {:?}", e);
                        Err(e.into())
                    }
                }
            }
            Err(e) => {
                println!("Reqwest status error: {:?}", e.status());
                println!("URL: {:?}", e.url());
                Err(e.into())
            }
        }
    }
}
