pub mod orderbook {
    tonic::include_proto!("orderbook");
}

use futures::stream::Stream;
use std::time::Duration;
use tokio_stream::StreamExt;
use tonic::transport::Channel;

use orderbook::{orderbook_aggregator_client::OrderbookAggregatorClient, Empty};


async fn streaming_orderbook_aggregator(client: &mut OrderbookAggregatorClient<Channel>, num: usize) {
    let stream = client
        .book_summary(Empty {})
        .await
        .unwrap()
        .into_inner();

    // stream is infinite - take just 5 elements and then disconnect
    let mut stream = stream.take(num);
    while let Some(item) = stream.next().await {
        println!("\treceived: {:?}", item.unwrap());
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = OrderbookAggregatorClient::connect("http://[::1]:50051").await.unwrap();

    println!("Streaming echo:");
    streaming_orderbook_aggregator(&mut client, 5).await;
    tokio::time::sleep(Duration::from_secs(10)).await; //do not mess server println functions
    Ok(())
}