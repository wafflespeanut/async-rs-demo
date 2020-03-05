use crate::codegen::{
    self, Summary,
    aggregator_server::{Aggregator, AggregatorServer},
};
use crate::depth::{
    BinanceSocket, DepthProvider, Merger,
};
use crate::error::AggregatorError;
use futures::channel::mpsc;
use futures::stream::{Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

use std::net::SocketAddr;
use std::pin::Pin;

struct Service {
    merger: Merger,
}

#[tonic::async_trait]
impl Aggregator for Service {
    type BookSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send + Sync + 'static>>;

    async fn book_summary(
        &self,
        req: Request<codegen::Request>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        info!("Incoming connection from {:?}",
              req.remote_addr().map(|a| a.to_string()).unwrap_or("[unknown]".into()));

        let req = req.into_inner();
        let mut stream = self.merger.add_client(req.symbol).await;
        let out = async_stream::stream! {
            while let Some(summary) = stream.next().await {
                yield Ok(summary);
            }
        };

        Ok(Response::new(Box::pin(out) as Self::BookSummaryStream))
    }
}

/// Initializes the gRPC service for orderbook aggregation.
pub async fn serve(addr: SocketAddr) -> Result<(), AggregatorError> {
    let providers = vec![
        BinanceSocket::default(),
    ];

    let mut action_senders = vec![];
    // Spawn a task for each provider.
    let book_receivers = providers.into_iter().map(|mut provider| {
        let (action_tx, action_rx) = mpsc::unbounded();
        let (book_tx, book_rx) = mpsc::unbounded();

        action_senders.push(action_tx.clone());
        tokio::spawn(async move {
            provider.start_processing(action_tx, action_rx, book_tx);
        });

        book_rx
    }).collect();

    let merger = Merger::new(action_senders);
    merger.start(book_receivers);

    info!("Listening for requests in {}", addr);
    Server::builder()
        .add_service(AggregatorServer::new(Service {
            merger,
        }))
        .serve(addr)
        .await?;
    Ok(())
}
