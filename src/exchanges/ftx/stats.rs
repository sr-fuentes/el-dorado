use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Latency {
    bursty: bool,
    p50: f64,
    request_count: i32,
}

pub type Latencies = Vec<Latency>;