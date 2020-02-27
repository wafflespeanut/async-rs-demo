mod codegen;
mod error;
mod util;

use self::codegen::aggregator_client::AggregatorClient;
use self::codegen::Empty;
use self::error::AggregatorError;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), AggregatorError> {
    let addr = util::addr_from_env()?;
    let mut client = AggregatorClient::connect(format!("http://{}", addr)).await?;
    let mut stream = client
        .book_summary(Request::new(Empty {}))
        .await?
        .into_inner();
    while let Some(summary) = stream.message().await? {
        println!("{:?}", summary);
    }

    Ok(())
}
