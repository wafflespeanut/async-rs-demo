use super::{DepthProvider, OrderBook, SocketState};
use crate::error::AggregatorError;
use tungstenite::Message;

use std::ops::{Deref, DerefMut};

const DEPTH_SUFFIX: &str = "@depth20";

#[derive(Default)]
pub struct BinanceSocket {
    state: SocketState,
    /// Incrementing index to track messages sent back and forth.
    idx: usize,
}

#[async_trait::async_trait]
impl DepthProvider for BinanceSocket {

    fn identifier(&self) -> &'static str {
        "binance"
    }

    /// URL for combined streams in Binance.
    fn base_url(&self) -> &'static str {
        "wss://stream.binance.com:9443/stream"
    }

    fn reset(&mut self) {
        self.idx = 0;
    }

    fn create_subscription_message(&mut self, _symbol: &str) -> Message {
        let bytes = serde_json::to_vec(&Request {
            id: self.idx,
            method: Method::Subscribe,
            // Subscriptions are "PUT" in binance, so we need all the existing subscriptions.
            params: self
                .state
                .subscriptions
                .iter()
                .map(String::as_str)
                .map(|s| format!("{}{}", s, DEPTH_SUFFIX))
                .collect(),
        })
        .expect("serializing subscription request");
        self.idx += 1;
        Message::Binary(bytes)
    }

    fn process_message(&mut self, bytes: &[u8]) -> Option<OrderBook> {
        if let Ok(resp) = serde_json::from_slice::<Response>(&bytes) {
            if resp.stream.ends_with(DEPTH_SUFFIX) {
                let symbol = &resp.stream[..DEPTH_SUFFIX.len()];

                return Some(OrderBook {
                    exchange: self.identifier(),
                    symbol: symbol.into(),
                    bids: resp
                        .data
                        .bids
                        .into_iter()
                        .filter_map(|(p, q)| p.parse().and_then(|p| q.parse().map(|q| (p, q))).ok())
                        .collect(),
                    asks: resp
                        .data
                        .asks
                        .into_iter()
                        .filter_map(|(p, q)| p.parse().and_then(|p| q.parse().map(|q| (p, q))).ok())
                        .collect(),
                });
            }
        }

        None
    }
}

/* Models */

#[derive(Serialize)]
struct Request {
    id: usize,
    method: Method,
    params: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "UPPERCASE")]
enum Method {
    Subscribe,
}

#[derive(Deserialize)]
struct Response {
    stream: String,
    data: ResponseOrderBook,
}

#[derive(Deserialize)]
struct ResponseOrderBook {
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

/* Other impls */

impl Deref for BinanceSocket {
    type Target = SocketState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for BinanceSocket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
