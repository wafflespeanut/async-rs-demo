use crate::error::AggregatorError;

use std::env;
use std::net::SocketAddr;

const ENV_GRPC_ADDR: &str = "GRPC_ADDR";

/// Fetches service address from environment or falls back to default.
pub fn addr_from_env() -> Result<SocketAddr, AggregatorError> {
    env::var(ENV_GRPC_ADDR)
        .or_else(|_| Ok(String::from("[::1]:9051")))
        .and_then(|v| {
            v.parse()
                .map_err(|_| AggregatorError::ParseEnvVar(ENV_GRPC_ADDR))
        })
}
