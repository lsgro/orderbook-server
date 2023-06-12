//! Protobuf RPC server for continuously updated snapshots of a trading book
//! consolidated from multiple exchanges.

use log::{LevelFilter, info};
use simple_logger::SimpleLogger;
use futures::Stream;
use std::{env, pin::Pin, net, str::FromStr};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

use keyrock_eu_lsgro::orderbook::{Summary, Empty, orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer}};

use keyrock_eu_lsgro::cli::ArgParser;
use keyrock_eu_lsgro::exchange::{BookUpdateSource, BookUpdateStream};
use keyrock_eu_lsgro::service::BookSummaryService;
use keyrock_eu_lsgro::binance::BinanceBookUpdateSource;
use keyrock_eu_lsgro::bitstamp::BitstampBookUpdateSource;

type ResponseStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;
type SummaryResult = Result<Response<ResponseStream>, Status>;


const USAGE_MESSAGE: &'static str = "Usage: server <currency pair> [port]";


/// Top level object representing a Profobuf RPC server.
pub struct ProtobufOrderbookServer {
    /// The exchange adapters.
    sources: Vec<Box<dyn BookUpdateSource>>,
}

impl ProtobufOrderbookServer {
    /// Create a new [ProtobufOrderbookServer](ProtobufOrderbookServer) object.
    ///
    /// # Arguments
    ///
    /// * `sources` - A [vector](Vec) of [BookUpdateSource](BookUpdateSource) objects, one
    /// for each exchange.
    ///
    /// # Returns
    ///
    /// A [ProtobufOrderbookServer](ProtobufOrderbookServer) object.
    pub fn new(sources: Vec<Box<dyn BookUpdateSource>>) -> Self {
        Self { sources }
    }

    /// Start the Protobuf RPC server on a port.
    ///
    /// # Arguments
    ///
    /// * `port` - The TCP port of the server.
    ///
    /// # Returns
    ///
    /// An empty [Result](Result).
    pub async fn serve(self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let our_address = net::SocketAddr::new(
            net::IpAddr::V6(net::Ipv6Addr::from_str("::1").unwrap()),
            port
        );
        Server::builder()
            .add_service(OrderbookAggregatorServer::new(self))
            .serve(our_address)
            .await
            .unwrap();
        Ok(())
    }
}

/// Implementation of the trait automatically generated from the file `proto/orderbook.proto`.
#[tonic::async_trait]
impl OrderbookAggregator for ProtobufOrderbookServer {

    type BookSummaryStream = ResponseStream;

    async fn book_summary(&self, req: Request<Empty>) -> SummaryResult {
        info!("OrderbookServer::book_summary");
        info!("Client connected from: {:?}", req.remote_addr());

        let (tx, rx) = mpsc::channel(128);
        let book_update_stream = BookUpdateStream::new(&self.sources).await;
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
    let mut arg_parser = ArgParser::new(env::args(), USAGE_MESSAGE);
    let product = arg_parser.extract_currency_pair();
    let port = arg_parser.extract_port();
    let binance_source = BinanceBookUpdateSource::new(&product);
    let bitstamp_source = BitstampBookUpdateSource::new(&product);
    let sources: Vec<Box<dyn BookUpdateSource>> = vec![
        Box::new(binance_source),
        Box::new(bitstamp_source)
    ];
    let server = ProtobufOrderbookServer::new(sources);
    server.serve(port).await
}