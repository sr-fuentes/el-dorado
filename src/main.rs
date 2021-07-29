use el_dorado::exchanges::ftx;

#[tokio::main]
async fn main() {
    let client = ftx::client::FtxRest::new_us();
    client.test();
}
