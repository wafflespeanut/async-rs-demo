use super::{OrderBook, Processor, SocketState};

use std::f64;
use std::ops::{Deref, DerefMut};

const STREAM_SUFFIX: &str = "@depth20@100ms";

/// Websocket for Binance.
/// https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md
#[derive(Default)]
pub struct BinanceSocket {
    state: SocketState,
    /// Incrementing index to track messages sent back and forth.
    idx: usize,
}

#[async_trait::async_trait]
impl Processor for BinanceSocket {
    type Response = Response;

    fn identifier(&self) -> &'static str {
        "binance"
    }

    /// URL for combined streams in Binance.
    /// https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#general-wss-information
    fn base_url(&self) -> &'static str {
        "wss://stream.binance.com:9443/stream"
    }

    fn reset(&mut self) {
        self.idx = 0;
    }

    fn create_subscription_message(&mut self, _symbol: &str) -> String {
        // Subscriptions are "PUT" in binance, so we need all the existing subscriptions.
        // https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#subscribe-to-a-stream
        let string = serde_json::to_string(&Request {
            id: self.idx,
            method: Method::Subscribe,
            params: self
                .state
                .subscriptions
                .iter()
                .map(String::as_str)
                .map(|s| format!("{}{}", s, STREAM_SUFFIX))
                .collect(),
        })
        .expect("serializing subscription request");
        self.idx += 1;
        string
    }

    fn process_message(&mut self, resp: Self::Response) -> OrderBook {
        let symbol = if resp.stream.ends_with(STREAM_SUFFIX) {
            String::from(&resp.stream[..(resp.stream.len() - STREAM_SUFFIX.len())])
        } else {
            // If it's invalid, we'll filter it later anyway.
            resp.stream
        };

        OrderBook {
            exchange: self.identifier(),
            symbol,
            bids: resp
                .data
                .bids
                .into_iter()
                .filter_map(|(p, q)| match (p.parse(), q.parse()) {
                    (Ok(p), Ok(q)) if p != f64::NAN && q != f64::NAN => Some((p, q)),
                    _ => None,
                })
                .collect(),
            asks: resp
                .data
                .asks
                .into_iter()
                .filter_map(|(p, q)| match (p.parse(), q.parse()) {
                    (Ok(p), Ok(q)) if p != f64::NAN && q != f64::NAN => Some((p, q)),
                    _ => None,
                })
                .collect(),
        }
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
pub struct Response {
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
