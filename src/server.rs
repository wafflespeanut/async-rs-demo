mod codegen;
mod error;
mod rpc;
mod util;

use self::error::AggregatorError;

#[tokio::main]
async fn main() -> Result<(), AggregatorError> {
    let addr = util::addr_from_env()?;
    rpc::serve(addr).await
}
