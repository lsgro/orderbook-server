pub mod orderbook {
    tonic::include_proto!("orderbook");
}

use futures::Stream;
use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};

use orderbook::{Empty, Summary, Level, orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer}};


type ResponseStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;
type SummaryResult = Result<Response<ResponseStream>, Status>;

pub struct OrderbookServer;

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookServer {

    type BookSummaryStream = ResponseStream;

    async fn book_summary(&self, req: Request<Empty>) -> SummaryResult {
        println!("OrderbookServer::book_summary");
        println!("\tclient connected from: {:?}", req.remote_addr());

        // creating infinite stream with requested message
        let repeat = std::iter::repeat(Summary {
            spread: 2.1,
            bids: vec![
                Level { exchange: "binance".to_string(), price: 100.0, amount: 0.5 },
                Level { exchange: "binance".to_string(), price: 99.0, amount: 0.6 },
            ],
            asks: vec![
                Level { exchange: "bitstamp".to_string(), price: 102.1, amount: 0.4 },
                Level { exchange: "binance".to_string(), price: 104.0, amount: 0.6 },
            ],
        });
        let mut stream = Box::pin(tokio_stream::iter(repeat).throttle(Duration::from_millis(200)));

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            println!("\tclient disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::BookSummaryStream
        ))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = OrderbookServer {};
    Server::builder()
        .add_service(orderbook::orderbook_aggregator_server::OrderbookAggregatorServer::new(server))
        .serve("[::1]:50051".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();
    Ok(())
}