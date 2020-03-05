use super::{DepthProvider, DepthSnapshot};
use crate::error::AggregatorError;
use futures::stream::{SplitSink, SplitStream, StreamExt};
use futures::sink::SinkExt;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use tungstenite::Message as WsMessage;

type WsStream = WebSocketStream<TcpStream>;

pub struct BinanceSocket {
    reader: SplitStream<WsStream>,
    writer: SplitSink<WsStream, WsMessage>,
}

#[async_trait::async_trait]
impl DepthProvider for BinanceSocket {
    async fn for_symbol(symbol: &str) -> Result<Self, AggregatorError> {
        let url = format!(
            "wss://stream.binance.com:9443/ws/{}@depth20@100ms",
            symbol.to_lowercase()
        );
        let (stream, _resp) = tokio_tungstenite::connect_async(url).await?;
        let (writer, reader) = stream.split();
        Ok(BinanceSocket { reader, writer })
    }

    async fn get_message(&mut self) -> Result<DepthSnapshot, AggregatorError> {
        trace!("Waiting for message.");
        loop {
            match self.reader.next().await {
                // Server sends a "ping" frame every 3 minutes and expects a "pong" frame back.
                // https://github.com/binance-exchange/binance-official-api-docs/blob/master/web-socket-streams.md#general-wss-information
                Some(Ok(WsMessage::Ping(bytes))) => {
                    debug!("Received ping. Responding with pong.");
                    if let Err(e) = self.writer.send(WsMessage::Pong(bytes)).await {
                        error!("Error responding with pong: {:?}", e);
                    }
                }
                Some(Ok(WsMessage::Binary(bytes))) => {
                    // NOTE: There's no failsafe mechanism here.
                    if let Ok(snapshot) = serde_json::from_slice(&bytes) {
                        return Ok(snapshot);
                    }

                    if log::log_enabled!(log::Level::Debug) {
                        warn!("Error decoding message: {}", String::from_utf8_lossy(&bytes));
                    }
                }
                _ => (),
            }
        }
    }
}
