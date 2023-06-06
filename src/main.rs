use tokio::runtime::Runtime;
use log::LevelFilter;
use simple_logger::SimpleLogger;

use keyrock_eu_lsgro::core::*;
use keyrock_eu_lsgro::binance::BinanceProvider;

//tmp
use futures::prelude::*;

fn main() {
    SimpleLogger::new().with_level(LevelFilter::Info).init().unwrap();
    let rt = Runtime::new().unwrap();
    let mut binance_provider = BinanceProvider::new(CurrencyPair { main: "ETH", counter: "BTC" });
    rt.block_on(async {
        match binance_provider.connect().await {
            Ok(BookStream(mut stream)) => {
                println!("A book stream!");
                for _ in 0..10 {
                    match stream.next().await {
                        Some(book_update) => println!("{:?}", book_update),
                        None => println!("No message received"),
                    }
                }
            },
            Err(error) => println!("Error {:?}", error),
        }
        println!("Hello, world!")
    });
}
