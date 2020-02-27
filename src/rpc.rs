use crate::codegen::aggregator_server::{Aggregator, AggregatorServer};
use crate::codegen::{Empty, Summary};
use crate::error::AggregatorError;
use tokio::sync::mpsc;
use tonic::{transport::Server, Request, Response, Status};

use std::net::SocketAddr;

#[derive(Debug)]
struct Service;

#[tonic::async_trait]
impl Aggregator for Service {
    type BookSummaryStream = mpsc::Receiver<Result<Summary, Status>>;

    async fn book_summary(
        &self,
        _req: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        unimplemented!();
    }
}

/// Initializes the gRPC service for orderbook aggregation.
pub async fn serve(addr: SocketAddr) -> Result<(), AggregatorError> {
    println!("Listening for requests in {}", addr);
    Server::builder()
        .add_service(AggregatorServer::new(Service))
        .serve(addr)
        .await?;
    Ok(())
}
