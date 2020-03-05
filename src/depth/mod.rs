mod binance;

pub use self::binance::BinanceSocket;

use crate::error::AggregatorError;
use futures::channel::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use futures::sink::SinkExt;
use futures::stream::{self, SplitSink, SplitStream, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message;

use std::collections::HashMap;
use std::iter;
use std::ops::DerefMut;
use std::time::Duration;

const CONN_FAIL_DELAY_SECS: u64 = 5;

type WsStream = WebSocketStream<TcpStream>;

/// Represents an order book depth provider.
#[async_trait::async_trait]
pub trait DepthProvider: DerefMut<Target = SocketState> + Send + Sync + 'static {

    /// Base URL for the websocket.
    fn base_url(&self) -> &str;

    /// Reset the internal state of this provider. This is called when we
    /// recreate a connection.
    fn reset(&mut self);

    /// Creates the websocket message for subscribing to a symbol.
    /// This symbol will already exist in the underlying socket state,
    /// so implementors shouldn't modify the state by themselves.
    fn create_subscription_message(&mut self, symbol: &str) -> Message;

    /// We've received binary message from the websocket. Process and return
    /// the order book, if any.
    fn process_message(&mut self, bytes: &[u8]) -> Option<OrderBook>;

    async fn start_processing(
        &mut self,
        mut action_sender: UnboundedSender<ProviderAction>,
        action_receiver: UnboundedReceiver<ProviderAction>,
        mut book_stream_sender: UnboundedSender<BookStream>,
    ) {
        let stream = self.attempt_connection().await;
        let (mut writer, reader) = stream.split();
        let mut combined = stream::select(
            reader
                .map(ProcessOutput::Socket)
                .chain(stream::iter(iter::once(ProcessOutput::Reconnect))),
            action_receiver.map(ProcessOutput::Action),
        );

        loop {
            match combined.next().await {
                // If we receive a ping from server, then send a pong.
                Some(ProcessOutput::Socket(Ok(Message::Ping(bytes)))) => {
                    if let Err(e) = writer.send(Message::Pong(bytes)).await {
                        error!("Error responding with pong: {:?}", e);
                    }
                }
                // If we receive a binary message from server, then try to get the orderbook
                // from it and log it otherwise.
                Some(ProcessOutput::Socket(Ok(Message::Binary(bytes)))) => {
                    if let Some(book) = self.process_message(&bytes) {
                        if let Some(sender) = self.subscriptions.get_mut(&book.symbol) {
                            // We're sure that we'll consume the books properly.
                            let _ = sender.send(book).await;
                        } else if log::log_enabled!(log::Level::Debug) {
                            warn!("Received order book for unknown symbol: {}", &book.symbol);
                        }

                        continue;
                    }

                    if log::log_enabled!(log::Level::Debug) {
                        warn!(
                            "Error decoding message: {}",
                            String::from_utf8_lossy(&bytes)
                        );
                    }
                }
                // If we receive a subscription request from the service, then
                // issue a new subscription if needed.
                Some(ProcessOutput::Action(ProviderAction::Subscribe { symbol, replace })) => {
                    let symbol = symbol.to_lowercase();
                    if self.subscriptions.get(&symbol).is_some() && !replace {
                        continue;
                    }

                    // We're using a bounded channel because we know we know that the service
                    // can consume almost immediately. But still, let's have some buffer.
                    let (sender, receiver) = mpsc::channel(10);
                    self.subscriptions.insert(symbol.clone(), sender);
                    let msg = self.create_subscription_message(&symbol);

                    if let Err(e) = writer.send(msg).await {
                        error!("Error subscribing to symbol {}: {:?}", symbol, e);
                        self.subscriptions.remove(&symbol);
                    }

                    let _ = book_stream_sender
                        .send(BookStream { symbol, receiver })
                        .await;
                }
                // If we get disconnected accidentally or intentionally, reconnect again.
                Some(ProcessOutput::Reconnect)
                | Some(ProcessOutput::Socket(Err(_)))
                | Some(ProcessOutput::Socket(Ok(Message::Close(_)))) => {
                    info!(
                        "Connection closed for {:?}. Reconnecting and restoring subscriptions...",
                        self.base_url()
                    );
                    let stream = self.attempt_connection().await;
                    let (w, r) = stream.split();
                    writer = w;
                    *combined.get_mut().0 = r
                        .map(ProcessOutput::Socket)
                        .chain(stream::iter(iter::once(ProcessOutput::Reconnect)));

                    self.reset();
                    // Notify self to resubscribe existing subscriptions.
                    for (symbol, _) in self.subscriptions.drain() {
                        let _ = action_sender
                            .unbounded_send(ProviderAction::Subscribe {
                                symbol,
                                replace: true,
                            });
                    }
                }
                _ => (),
            }
        }
    }

    /// Attempt connection until it succeeds.
    async fn attempt_connection(&self) -> WsStream {
        loop {
            match tokio_tungstenite::connect_async(self.base_url()).await {
                Ok((s, _resp)) => return s,
                Err(e) => error!(
                    "Connection failed for {:?}: {:?}. Retrying in {} seconds.",
                    self.base_url(),
                    e,
                    CONN_FAIL_DELAY_SECS
                ),
            }

            tokio::time::delay_for(Duration::from_secs(CONN_FAIL_DELAY_SECS)).await;
        }
    }
}

/// Message sent to this provider.
pub enum ProviderAction {
    Subscribe { symbol: String, replace: bool },
    // TODO: Support unsubscribing.
}

/// Abstraction for a websocket-related state. This contains:
/// - Subscriptions containing (lowercase'd) symbols and the MPSC sender which
/// streams the order book.
#[derive(Default)]
pub struct SocketState {
    subscriptions: HashMap<String, Sender<OrderBook>>,
}

/// An order book containing the asks and bids returned by the stream. The exact number
/// of which is unspecified, since different streams return different numbers of bids and asks.
// NOTE: As soon as we parse the string as floats, we'll encounter floating point errors.
// Maybe we should use them only for comparison and maintain them as strings? Does it matter?
pub struct OrderBook {
    symbol: String,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

/// Represents a stream of order books for a symbol.
pub struct BookStream {
    symbol: String,
    receiver: Receiver<OrderBook>,
}

/// Internal enum for selecting over streams.
enum ProcessOutput {
    Socket(Result<Message, tungstenite::Error>),
    Action(ProviderAction),
    Reconnect,
}
