use log::{LevelFilter, info};
use simple_logger::SimpleLogger;
use futures::Stream;
use std::pin::Pin;
use std::net::{SocketAddr, IpAddr, Ipv6Addr};
use std::str::FromStr;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};
use keyrock_eu_lsgro::binance::make_binance_provider;
use keyrock_eu_lsgro::bitstamp::make_bitstamp_provider;

use keyrock_eu_lsgro::orderbook::{Summary, Empty, orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer}};

use keyrock_eu_lsgro::core::CurrencyPair;
use keyrock_eu_lsgro::service::{BookSummaryService, BookUpdateStream};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;
type SummaryResult = Result<Response<ResponseStream>, Status>;


pub struct ProtobufOrderbookServer {
    product: CurrencyPair,
}

impl ProtobufOrderbookServer {
    pub fn new(product: CurrencyPair) -> Self {
        Self { product }
    }

    pub async fn serve(self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let our_address = SocketAddr::new(IpAddr::V6(Ipv6Addr::from_str("::1").unwrap()), port);
        Server::builder()
            .add_service(OrderbookAggregatorServer::new(self))
            .serve(our_address)
            .await
            .unwrap();
        Ok(())
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for ProtobufOrderbookServer {

    type BookSummaryStream = ResponseStream;

    async fn book_summary(&self, req: Request<Empty>) -> SummaryResult {
        info!("OrderbookServer::book_summary");
        info!("Client connected from: {:?}", req.remote_addr());

        let (tx, rx) = mpsc::channel(128);
        let binance_provider = make_binance_provider(&self.product);
        let bitstamp_provider = make_bitstamp_provider(&self.product);
        let book_update_stream = BookUpdateStream::new(vec![binance_provider, bitstamp_provider]).await;
        let mut service: BookSummaryService = BookSummaryService::new(book_update_stream);

        tokio::spawn(async move {
            while let Some(item) = service.next().await {
                if let Err(_) = tx.send(Result::<Summary, Status>::Ok(item)).await {
                    break;
                }
            }
            info!("Client disconnected");
            service.disconnect().await;
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::BookSummaryStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    SimpleLogger::new().with_level(LevelFilter::Info).init().unwrap();
    let server = ProtobufOrderbookServer::new(CurrencyPair { main: "ETH", counter: "BTC" });
    server.serve(50051).await
}