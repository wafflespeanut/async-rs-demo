mod binance;

use crate::error::AggregatorError;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DepthSnapshot {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[async_trait::async_trait]
pub trait DepthProvider: Sized {

    async fn for_symbol(symbol: &str) -> Result<Self, AggregatorError>;

    async fn get_message(&mut self) -> Result<DepthSnapshot, AggregatorError>;
}
