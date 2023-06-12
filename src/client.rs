//! Example client for the Protobuf RPC server.

use std::env;
use log::{LevelFilter, info};
use simple_logger::SimpleLogger;
use tokio_stream::StreamExt;

use orderbook_server::orderbook::{orderbook_aggregator_client::OrderbookAggregatorClient, Empty};
use orderbook_server::cli::ArgParser;


const USAGE_MESSAGE: &str = "Usage: client <#messages> [port]";


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    SimpleLogger::new().with_level(LevelFilter::Info).init().unwrap();
    let mut arg_parser = ArgParser::new(env::args(), USAGE_MESSAGE);
    let message_num = arg_parser.extract_message_num();
    let port = arg_parser.extract_port();
    let server_url = format!("http://[::1]:{}", port);
    let mut client = OrderbookAggregatorClient::connect(server_url.clone()).await.unwrap_or_else(
        |_| panic!("Could not connect to server at {}", &server_url)
    );
    info!("Streaming orderbook for {} messages", message_num);
    let stream = client
        .book_summary(Empty {})
        .await
        .unwrap()
        .into_inner();
    let mut finite_stream = stream.take(message_num);
    while let Some(item) = finite_stream.next().await {
        info!("{:?}", item.unwrap());
    }
    Ok(())
}