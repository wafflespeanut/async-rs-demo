use crate::codegen::aggregator_server::{Aggregator, AggregatorServer};
use crate::codegen::{Empty, Summary};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

#[derive(Debug)]
struct Service;

#[tonic::async_trait]
impl Aggregator for Service {

    type BookSummaryStream = mpsc::Receiver<Result<Summary, Status>>;

    async fn book_summary(&self, req: Request<Empty>) -> Result<Response<Self::BookSummaryStream>, Status> {
        unimplemented!();
    }
}
