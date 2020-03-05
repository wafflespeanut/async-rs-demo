mod binance;

use crate::error::AggregatorError;
use futures::channel::mpsc::{self, Sender, Receiver};
use futures::stream::{SplitSink, SplitStream, StreamExt};
use futures::sink::SinkExt;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message as WsMessage;

use std::collections::HashMap;
use std::ops::DerefMut;

type WsStream = WebSocketStream<TcpStream>;

#[async_trait::async_trait]
pub trait DepthProvider: DerefMut<Target=Socket> {

    async fn create_subscription_message(&mut self, symbol: &str) -> Result<WsMessage, AggregatorError>;

    async fn subscribe(&mut self, symbol: &str) -> Result<Receiver<OrderBook>, AggregatorError> {
        let symbol = symbol.to_lowercase();
        if self.subscriptions.get(&symbol).is_some() {
            return Err(AggregatorError::SubscriptionExists(symbol));
        }

        let msg = self.create_subscription_message(&symbol).await?;
        self.writer.send(msg).await?;

        // We're using a bounded channel because we know we know that the service
        // can consume almost immediately. But still, let's have some buffer.
        let (sender, receiver) = mpsc::channel(10);
        self.subscriptions.insert(symbol, sender);
        Ok(receiver)
    }

    async fn get_message(&mut self) -> Result<OrderBook, AggregatorError>;
}

#[async_trait::async_trait]
pub trait DepthProviderInit: DepthProvider + From<Socket> + Send {

    const BASE_URL: &'static str;

    async fn connect_socket() -> Result<Self, AggregatorError> {
        let (stream, _resp) = tokio_tungstenite::connect_async(Self::BASE_URL).await?;
        let (writer, reader) = stream.split();
        Ok(Socket { reader, writer, subscriptions: HashMap::new(), }.into())
    }
}

pub struct Socket  {
    reader: SplitStream<WsStream>,
    writer: SplitSink<WsStream, WsMessage>,
    subscriptions: HashMap<String, Sender<OrderBook>>,
}

pub struct OrderBook {
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}
