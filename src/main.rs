use el_dorado::exchanges::ftx::RestClient;

#[tokio::main]
async fn main() {
    let client = RestClient::new_us();
    println!("{}", client.endpoint);
    //client.request().await;
}
