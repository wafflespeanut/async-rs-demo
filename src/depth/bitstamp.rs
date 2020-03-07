use super::{OrderBook, Processor, SocketState};

use std::f64;
use std::ops::{Deref, DerefMut};

const CHANNEL_PREFIX: &str = "detail_order_book_";

/// Websocket for Bitstamp.
/// https://www.bitstamp.net/websocket/v2/
#[derive(Default)]
pub struct BitstampSocket {
    state: SocketState,
}

#[async_trait::async_trait]
impl Processor for BitstampSocket {
    type Response = Response;

    fn identifier(&self) -> &'static str {
        "bitstamp"
    }

    fn base_url(&self) -> &'static str {
        "wss://ws.bitstamp.net"
    }

    fn create_subscription_message(&mut self, symbol: &str) -> String {
        serde_json::to_string(&Request {
            event: Event::Subscribe,
            data: RequestData::Subscription {
                channel: format!("{}{}", CHANNEL_PREFIX, symbol),
            },
        })
        .expect("serializing subscription request")
    }

    fn process_message(&mut self, resp: Self::Response) -> OrderBook {
        let symbol = if resp.channel.starts_with(CHANNEL_PREFIX) {
            String::from(&resp.channel[CHANNEL_PREFIX.len()..])
        } else {
            // If it's invalid, we'll filter it later anyway.
            resp.channel
        };

        OrderBook {
            exchange: self.identifier(),
            symbol,
            bids: resp
                .data
                .bids
                .into_iter()
                .filter_map(|(p, q, _)| match (p.parse(), q.parse()) {
                    (Ok(p), Ok(q)) if p != f64::NAN && q != f64::NAN => Some((p, q)),
                    _ => None,
                })
                .collect(),
            asks: resp
                .data
                .asks
                .into_iter()
                .filter_map(|(p, q, _)| match (p.parse(), q.parse()) {
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
    event: Event,
    data: RequestData,
}

#[derive(Serialize)]
enum Event {
    #[serde(rename = "bts:subscribe")]
    Subscribe,
}

#[derive(Serialize)]
#[serde(untagged)]
enum RequestData {
    Subscription { channel: String },
}

#[derive(Deserialize)]
pub struct Response {
    channel: String,
    data: ResponseOrderBook,
}

#[derive(Deserialize)]
struct ResponseOrderBook {
    bids: Vec<(String, String, String)>,
    asks: Vec<(String, String, String)>,
}

/* Other impls */

impl Deref for BitstampSocket {
    type Target = SocketState;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for BitstampSocket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
