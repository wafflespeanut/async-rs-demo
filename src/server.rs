mod codegen;
mod error;
mod rpc;

use self::error::AggregatorError;

use std::env;

const ENV_GRPC_ADDR: &str = "GRPC_ADDR";

#[tokio::main]
async fn main() -> Result<(), AggregatorError> {
    let addr = env::var(ENV_GRPC_ADDR)
        .or_else(|_| Ok(String::from("[::1]:9051")))
        .and_then(|v| {
            v.parse()
                .map_err(|_| AggregatorError::ParseEnvVar(ENV_GRPC_ADDR))
        })?;

    rpc::serve(addr).await
}
