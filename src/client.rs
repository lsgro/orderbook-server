use log::{LevelFilter, info};
use simple_logger::SimpleLogger;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use keyrock_eu_lsgro::orderbook::{orderbook_aggregator_client::OrderbookAggregatorClient, Empty};


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
    let mut client = OrderbookAggregatorClient::connect("http://[::1]:50051").await.unwrap();
    info!("Streaming orderbook:");
    streaming_orderbook_aggregator(&mut client, 500).await;
    Ok(())
}