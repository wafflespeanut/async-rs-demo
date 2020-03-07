use crate::codegen::{
    self,
    aggregator_server::{Aggregator, AggregatorServer},
    Summary,
};
use crate::depth::{BinanceSocket, DepthProcessor, Merger};
use crate::error::AggregatorError;
use futures::stream::{Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

use std::net::SocketAddr;
use std::pin::Pin;

struct Service {
    merger: Merger,
}

#[tonic::async_trait]
impl Aggregator for Service {
    type BookSummaryStream =
        Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send + Sync + 'static>>;

    async fn book_summary(
        &self,
        req: Request<codegen::Request>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        let client_id = Uuid::new_v4();
        info!(
            "Incoming connection from {:?} (client ID: {})",
            req.remote_addr()
                .map(|a| a.to_string())
                .unwrap_or("[unknown]".into()),
            client_id,
        );

        let req = req.into_inner();
        let mut stream = self.merger.add_client(client_id, req.symbol).await;
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
    let processors = vec![BinanceSocket::default()];

    let mut action_senders = vec![];
    // Spawn a task for each processor.
    let book_receivers = processors
        .into_iter()
        .map(|processor| {
            let (action_tx, book_rx) = processor.start();
            action_senders.push(action_tx.clone());
            book_rx
        })
        .collect();

    info!("Listening for requests in {}", addr);
    Server::builder()
        .add_service(AggregatorServer::new(Service {
            merger: Merger::start(action_senders, book_receivers),
        }))
        .serve(addr)
        .await?;
    Ok(())
}
