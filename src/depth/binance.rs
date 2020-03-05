use super::{DepthProvider, DepthProviderInit, OrderBook, Socket};
use crate::error::AggregatorError;
use futures::stream::StreamExt;
use futures::sink::SinkExt;
use tungstenite::Message;

use std::iter;
use std::ops::{Deref, DerefMut};

pub struct BinanceSocket {
    socket: Socket,
    /// Incrementing index to track messages sent back and forth.
    idx: usize,
}

#[async_trait::async_trait]
impl DepthProviderInit for BinanceSocket {

    /// URL for combined stream.
    const BASE_URL: &'static str = "wss://stream.binance.com:9443/stream";
}

#[async_trait::async_trait]
impl DepthProvider for BinanceSocket {

    async fn create_subscription_message(&mut self, symbol: &str) -> Result<Message, AggregatorError> {
        let bytes = serde_json::to_vec(&Request {
            id: self.idx,
            method: Method::Subscribe,
            // Subscriptions are replaced, so we need all the existing subscriptions.
            params: self.socket.subscriptions.keys().map(String::as_str).chain(iter::once(symbol))
                .map(|s| format!("{}@depth20", s))
                .collect(),
        })?;
        self.idx += 1;
        Ok(Message::Binary(bytes))
    }

    async fn get_message(&mut self) -> Result<OrderBook, AggregatorError> {
        trace!("Waiting for message.");
        loop {
            // Binance socket information - https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#general-wss-information
            match self.socket.reader.next().await {
                // Server sends a "ping" frame every 3 minutes and expects a "pong" frame back.
                Some(Ok(Message::Ping(bytes))) => {
                    debug!("Received ping. Responding with pong.");
                    if let Err(e) = self.socket.writer.send(Message::Pong(bytes)).await {
                        error!("Error responding with pong: {:?}", e);
                    }
                }
                Some(Ok(Message::Binary(bytes))) => {
                    if let Ok(resp) = serde_json::from_slice::<Response>(&bytes) {
                        return Ok(OrderBook {
                            bids: resp.data.bids,
                            asks: resp.data.asks,
                        });
                    }

                    if log::log_enabled!(log::Level::Debug) {
                        warn!("Error decoding message: {}", String::from_utf8_lossy(&bytes));
                    }
                }
                // Server disconnects every 24 hours. If we receive a close frame, then
                // reconnect again.
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                    *self = Self::connect_socket().await?;
                }
                _ => (),
            }
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
struct Response {
    stream: String,
    data: ResponseOrderBook,
}

#[derive(Deserialize)]
struct ResponseOrderBook {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

/* Other impls */

impl From<Socket> for BinanceSocket {
    fn from(s: Socket) -> Self {
        BinanceSocket {
            socket: s,
            idx: 0,
        }
    }
}

impl Deref for BinanceSocket {
    type Target = Socket;

    fn deref(&self) -> &Self::Target {
        &self.socket
    }
}

impl DerefMut for BinanceSocket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.socket
    }
}
