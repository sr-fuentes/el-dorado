use crate::exchanges::ftx::*;

pub async fn pull_markets_from_exchange(exchange: &str) -> Result<Vec<Market>, RestError> {
    // Get Rest Client
    let client = match exchange {
        "ftx" => RestClient::new_us(),
        "ftxus" => RestClient::new_intl(),
        _ => panic!("No client exists for {}.", exchange),
    };

    // Get Markets
    let markets = client.get_markets().await?;
    Ok(markets)
}
