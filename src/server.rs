//! Protobuf RPC server for continuously updated snapshots of a trading book
//! consolidated from multiple exchanges.

use log::{LevelFilter, info};
use simple_logger::SimpleLogger;
use futures::Stream;
use std::{env, pin::Pin, net, str::FromStr};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};

use orderbook_server::orderbook::{Summary, Empty, orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer}};

use orderbook_server::cli::ArgParser;
use orderbook_server::exchange::{ExchangeAdapter, BookUpdateStream};
use orderbook_server::service::BookSummaryService;
use orderbook_server::binance::make_binance_exchange_adapter;
use orderbook_server::bitstamp::make_bitstamp_echange_adapter;

type ResponseStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;
type SummaryResult = Result<Response<ResponseStream>, Status>;


const USAGE_MESSAGE: &str = "Usage: server <currency pair> [port]";


/// Top level object representing a Profobuf RPC server.
pub struct ProtobufOrderbookServer {
    /// The exchange adapters.
    exchange_adapters: Vec<ExchangeAdapter>,
}

impl ProtobufOrderbookServer {
    /// Create a new [ProtobufOrderbookServer](ProtobufOrderbookServer) object.
    ///
    /// # Arguments
    ///
    /// * `exchange_adapters` - A [vector](Vec) of [ExchangeAdapter](ExchangeAdapter) objects, one
    /// for each exchange.
    ///
    /// # Returns
    ///
    /// A [ProtobufOrderbookServer](ProtobufOrderbookServer) object.
    pub fn new(exchange_adapters: Vec<ExchangeAdapter>) -> Self {
        Self { exchange_adapters }
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
        let book_update_stream = BookUpdateStream::new(&self.exchange_adapters).await;
        let mut service: BookSummaryService = BookSummaryService::new(book_update_stream);

        tokio::spawn(async move {
            while let Some(item) = service.next().await {
                if tx.send(Result::<Summary, Status>::Ok(item)).await.is_err() {
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
    let binance_adapter = make_binance_exchange_adapter(&product).await;
    let bitstamp_adapter = make_bitstamp_echange_adapter(&product).await;
    let exchange_adapters: Vec<ExchangeAdapter> = vec![
        binance_adapter,
        bitstamp_adapter,
    ];
    let server = ProtobufOrderbookServer::new(exchange_adapters);
    server.serve(port).await
}