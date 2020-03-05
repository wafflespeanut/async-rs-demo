#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

mod codegen;
mod depth;
mod error;
mod rpc;
mod util;

use self::error::AggregatorError;

#[tokio::main]
async fn main() -> Result<(), AggregatorError> {
    env_logger::init();
    let addr = util::addr_from_env()?;
    // TODO: structopt arg parsing.

    rpc::serve(addr).await
}
