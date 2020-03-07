mod codegen;
mod error;
mod util;

use self::codegen::aggregator_client::AggregatorClient;
use self::error::AggregatorError;
use tonic::Request;

use std::env;

const ENV_VAR_SYMBOL: &str = "SYMBOL";

#[tokio::main]
async fn main() -> Result<(), AggregatorError> {
    let symbol =
        env::var(ENV_VAR_SYMBOL).map_err(|_| AggregatorError::MissingEnvVar(ENV_VAR_SYMBOL))?;
    let addr = util::addr_from_env()?;
    let mut client = AggregatorClient::connect(format!("http://{}", addr)).await?;
    let mut stream = client
        .book_summary(Request::new(codegen::Request { symbol }))
        .await?
        .into_inner();

    while let Some(summary) = stream.message().await? {
        println!("{:?}", summary);
    }

    Ok(())
}
