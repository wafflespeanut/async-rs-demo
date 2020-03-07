mod binance;
mod merger;

pub use self::binance::BinanceSocket;
pub use self::merger::Merger;

use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures::sink::SinkExt;
use futures::stream::{self, StreamExt};
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tungstenite::Message;

use std::cmp::Ordering;
use std::collections::HashSet;
use std::iter;
use std::ops::DerefMut;
use std::time::Duration;

const CONN_FAIL_DELAY_SECS: u64 = 5;

// TODO: Add trait for representing websocket messages so that we can
// decouple tungstenite.
pub type WsMessage = Message;

/// Internal enum for selecting over streams.
#[derive(Debug)]
enum ProcessOutput {
    Socket(Result<Message, tungstenite::Error>),
    Action(ProcessorAction),
    Reconnect,
}

/// Represents an order book depth processor for a provider.
#[async_trait::async_trait]
pub trait DepthProcessor: DerefMut<Target = SocketState> + Sized + Send + Sync + 'static {
    /* Required from implementors */

    type DepthMessage: DeserializeOwned;

    // NOTE: We cannot use constants because `async_trait` works by wrapping the async
    // function with an actual method (from which we cannot call `Self::*`).

    /// Identifier for this provider.
    fn identifier(&self) -> &'static str;

    /// Base URL for the websocket.
    fn base_url(&self) -> &'static str;

    /// Reset the internal state of this processor. This is called when we
    /// recreate a connection.
    fn reset(&mut self);

    /// Creates the websocket message for subscribing to a symbol.
    /// This symbol will already exist in the underlying socket state,
    /// so implementors shouldn't modify the state by themselves.
    fn create_subscription_message(&mut self, symbol: &str) -> Message;

    /// We've received a valid message from the websocket. Process and return
    /// the order book. This will be called by `process_bytes` method.
    fn process_message(&mut self, msg: Self::DepthMessage) -> OrderBook;

    /* Provided methods. */

    /// Processes a websocket message and returns the order book, if any.
    fn process_bytes(&mut self, bytes: &[u8]) -> Option<OrderBook> {
        if let Ok(msg) = serde_json::from_slice(bytes) {
            // NOTE: We're not returning directly so that we can log below.
            let book = self.process_message(msg);
            if self.subscriptions.contains(&book.symbol) {
                return Some(book);
            } else if log::log_enabled!(log::Level::Debug) {
                warn!(
                    "Received order book for unknown symbol from {:?}: {}",
                    self.identifier(),
                    &book.symbol
                );
            }
        }

        if log::log_enabled!(log::Level::Debug) {
            warn!(
                "Error decoding message from {:?}: {}",
                self.identifier(),
                String::from_utf8_lossy(&bytes)
            );
        }

        None
    }

    /// Starts this processor in the background and returns a sender for sending messages
    /// to this processor and a receiver for receiving order books from this processor.
    fn start(
        mut self,
    ) -> (
        UnboundedSender<ProcessorAction>,
        UnboundedReceiver<OrderBook>,
    ) {
        // Unbounded channel because the providers usually support notifying only
        // every few milliseconds within which processors can respond to actions
        // and merger can take care of sorting the books (so we won't have any back-pressure).
        let (action_tx, action_rx) = mpsc::unbounded();
        let (book_tx, book_rx) = mpsc::unbounded();

        let action_sender = action_tx.clone();
        tokio::spawn(async move {
            debug!("Spawning task for processing {:?} data.", self.identifier());
            self.start_processing(action_tx, action_rx, book_tx).await;
        });

        (action_sender, book_rx)
    }

    /// Consumes the necessary channel halves and starts processing websocket events.
    ///
    /// **NOTE:** This only needs the receiver for getting messages from gRPC server,
    /// but it also takes a sender so that it can notify itself about certain events.
    async fn start_processing(
        &mut self,
        action_sender: UnboundedSender<ProcessorAction>,
        action_receiver: UnboundedReceiver<ProcessorAction>,
        book_sender: UnboundedSender<OrderBook>,
    ) {
        let stream = attempt_connection(self.base_url()).await;
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
                    debug!("Received ping from {:?}.", self.identifier());
                    if let Err(err) = writer.send(Message::Pong(bytes)).await {
                        error!(
                            "Error responding with pong to {:?}: {:?}",
                            self.identifier(),
                            err
                        );
                    }
                }
                // If we receive a binary/text message from server, then try to get the orderbook
                // from it and log it otherwise.
                Some(ProcessOutput::Socket(Ok(Message::Text(string)))) => {
                    if let Some(book) = self.process_bytes(string.as_bytes()) {
                        let _ = book_sender.unbounded_send(book.sorted());
                    }
                }
                Some(ProcessOutput::Socket(Ok(Message::Binary(bytes)))) => {
                    if let Some(book) = self.process_bytes(&bytes) {
                        let _ = book_sender.unbounded_send(book.sorted());
                    }
                }
                // If we receive a subscription request from the service, then
                // issue a new subscription if needed.
                Some(ProcessOutput::Action(ProcessorAction::Subscribe { symbol, replace })) => {
                    let symbol = symbol.to_lowercase();
                    if self.subscriptions.contains(&symbol) && !replace {
                        continue;
                    }

                    debug!(
                        "Received (re-)subscription message for symbol {:?} in {:?}",
                        symbol,
                        self.identifier()
                    );

                    self.subscriptions.insert(symbol.clone());
                    let msg = self.create_subscription_message(&symbol);

                    if let Err(err) = writer.send(msg).await {
                        error!(
                            "Error subscribing to symbol {:?} in {:?} exchange: {:?}",
                            symbol,
                            self.identifier(),
                            err
                        );
                    // This is usually a network error. We'll get an error soon,
                    // and we'll reconnect. Don't remove the symbol.
                    } else {
                        info!(
                            "Subscribed to symbol {:?} in {:?} exchange.",
                            symbol,
                            self.identifier()
                        );
                    }
                }
                // If we get disconnected accidentally or intentionally, reconnect again.
                r @ Some(ProcessOutput::Reconnect)
                | r @ Some(ProcessOutput::Socket(Err(_)))
                | r @ Some(ProcessOutput::Socket(Ok(Message::Close(_)))) => {
                    info!(
                        "Connection closed for {:?} ({:?}). Reconnecting and restoring subscriptions...",
                        r, self.base_url()
                    );
                    let stream = attempt_connection(self.base_url()).await;
                    let (w, r) = stream.split();
                    writer = w;
                    *combined.get_mut().0 = r
                        .map(ProcessOutput::Socket)
                        .chain(stream::iter(iter::once(ProcessOutput::Reconnect)));

                    self.reset();
                    // Notify self to resubscribe existing subscriptions.
                    for symbol in self.subscriptions.drain() {
                        let _ = action_sender.unbounded_send(ProcessorAction::Subscribe {
                            symbol,
                            replace: true,
                        });
                    }
                }
                _ => (),
            }
        }
    }
}

/// Attempt connection until it succeeds.
async fn attempt_connection(url: &str) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    debug!("Connecting to {:?}", url);
    loop {
        match tokio_tungstenite::connect_async(url).await {
            Ok((s, _resp)) => return s,
            Err(e) => error!(
                "Connection failed for {:?}: {:?}. Retrying in {} seconds.",
                url, e, CONN_FAIL_DELAY_SECS
            ),
        }

        tokio::time::delay_for(Duration::from_secs(CONN_FAIL_DELAY_SECS)).await;
    }
}

/// Message sent to this processor.
#[derive(Debug)]
pub enum ProcessorAction {
    Subscribe { symbol: String, replace: bool },
    // TODO: Support unsubscribing.
}

/// Abstraction for a websocket-related state. This contains:
/// - Subscriptions containing (lowercase'd) symbols and the MPSC sender which
/// streams the order book.
#[derive(Default)]
pub struct SocketState {
    subscriptions: HashSet<String>,
}

/// An order book containing the asks and bids returned by the stream. The exact number
/// of which is unspecified, since different streams return different numbers of bids and asks.
// NOTE: As soon as we parse the string as floats, we'll encounter floating point errors.
// Maybe we should use them only for comparison and maintain them as strings? Does it matter?
#[derive(Clone)]
pub struct OrderBook {
    pub exchange: &'static str,
    pub symbol: String,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

impl OrderBook {
    /// Consumes and returns the order book with bids and asks sorted appropriately.
    /// (i.e., highest bids first and lowest asks first). This assumes that NaN has
    /// been discarded by the provider's depth processor.
    fn sorted(mut self) -> Self {
        self.bids
            .sort_by(|(p1, a1), (p2, a2)| match p2.partial_cmp(p1).unwrap() {
                Ordering::Equal => a2.partial_cmp(a1).unwrap(),
                ord => ord,
            });

        self.asks
            .sort_by(|(p1, a1), (p2, a2)| match p1.partial_cmp(p2).unwrap() {
                Ordering::Equal => a2.partial_cmp(a1).unwrap(),
                ord => ord,
            });

        OrderBook {
            exchange: self.exchange,
            symbol: self.symbol,
            bids: self.bids,
            asks: self.asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OrderBook;

    #[test]
    fn test_order_book_sorting() {
        #[rustfmt::skip]
        let cases = vec![
            vec![(0.026172, 1.13), (0.026155, 64.0), (0.026169, 23.28)],
            vec![(0.026149, 25.0), (0.026162, 1.39), (0.026168, 0.01), (0.026162, 69.3)],
        ];

        #[rustfmt::skip]
        let expectations = vec![
            // Bids are sorted descending, whereas asks are sorted ascending.
            (vec![(0.026172, 1.13), (0.026169, 23.28), (0.026155, 64.0)],
             vec![(0.026155, 64.0), (0.026169, 23.28), (0.026172, 1.13)]),
            // If same price value is encountered, then the amount is compared
            // and highest amount is placed first (for asks and bids).
            (vec![(0.026168, 0.01), (0.026162, 69.3), (0.026162, 1.39), (0.026149, 25.0)],
             vec![(0.026149, 25.0), (0.026162, 69.3), (0.026162, 1.39), (0.026168, 0.01)]),
        ];

        for (case, (bids, asks)) in cases.into_iter().zip(expectations.into_iter()) {
            let book = OrderBook {
                exchange: "",
                symbol: String::new(),
                bids: case.clone(),
                asks: case.clone(),
            }
            .sorted();

            assert_eq!(book.bids, bids);
            assert_eq!(book.asks, asks);
        }
    }
}
