use crate::codegen::{
    self, Summary,
    aggregator_server::{Aggregator, AggregatorServer},
};
use crate::depth::{
    BinanceSocket, DepthProvider, OrderBook, ProviderAction,
};
use crate::error::AggregatorError;
use futures::channel::mpsc::{self, Receiver, Sender, UnboundedSender};
use futures::sink::SinkExt;
use futures::stream::SelectAll as StreamSelector;
use tonic::{transport::Server, Request, Response, Status};

use std::net::SocketAddr;
use std::sync::Arc;

struct Service {
    providers: Vec<UnboundedSender<ProviderAction>>,
    clients: Vec<Sender<Result<Summary, Status>>>,
}

#[tonic::async_trait]
impl Aggregator for Service {
    type BookSummaryStream = Receiver<Result<Summary, Status>>;

    async fn book_summary(
        &self,
        req: Request<codegen::Request>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        info!("Incoming connection from {:?}",
              req.remote_addr().map(|a| a.to_string()).unwrap_or("unknown".into()));
        let req = req.into_inner();
        for tx in &self.providers {
            let _ = tx.unbounded_send(ProviderAction::Subscribe { symbol: req.symbol.clone(), replace: false });
        }

        unimplemented!();
    }
}

/// Initializes the gRPC service for orderbook aggregation.
pub async fn serve(addr: SocketAddr) -> Result<(), AggregatorError> {
    let providers = vec![
        ("binance", BinanceSocket::default()),
    ];

    let mut action_senders = vec![];
    let book_stream_receivers = providers.into_iter().map(|(name, mut provider)| {
        let (action_tx, action_rx) = mpsc::unbounded();
        let (book_stream_tx, book_stream_rx) = mpsc::unbounded();

        action_senders.push(action_tx.clone());
        tokio::spawn(async move {
            provider.start_processing(action_tx, action_rx, book_stream_tx);
        });

        book_stream_rx
    }).collect::<Vec<_>>();

    info!("Listening for requests in {}", addr);
    Server::builder()
        .add_service(AggregatorServer::new(Service {
            providers: action_senders,
            clients: vec![],
        }))
        .serve(addr)
        .await?;
    Ok(())
}
