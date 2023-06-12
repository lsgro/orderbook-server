use std::env;
use log::{LevelFilter, info};
use simple_logger::SimpleLogger;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use keyrock_eu_lsgro::orderbook::{orderbook_aggregator_client::OrderbookAggregatorClient, Empty};
use keyrock_eu_lsgro::cli::ArgParser;


const USAGE_MESSAGE: &'static str = "Usage: client <#messages> [port]";


async fn streaming_orderbook_aggregator(client: &mut OrderbookAggregatorClient<Channel>, num: usize) {
    let stream = client
        .book_summary(Empty {})
        .await
        .unwrap()
        .into_inner();

    let mut stream = stream.take(num);
    while let Some(item) = stream.next().await {
        info!("Received: {:?}", item.unwrap());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    SimpleLogger::new().with_level(LevelFilter::Info).init().unwrap();
    let mut arg_parser = ArgParser::new(env::args(), USAGE_MESSAGE);
    let message_num = arg_parser.extract_message_num();
    let port = arg_parser.extract_port();
    let server_url = format!("http://[::1]:{}", port);
    let mut client = OrderbookAggregatorClient::connect(server_url.clone()).await.expect(
        &format!("Could not connect to server at {}", &server_url)
    );
    info!("Streaming orderbook for {} messages", message_num);
    streaming_orderbook_aggregator(&mut client, message_num).await;
    Ok(())
}