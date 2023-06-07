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
            Ok(mut book_update_stream) => {
                for n in 0..300 {
                    book_update_stream.next().await.map(|book_update| print!("X"));
                    if n % 160 == 0 {
                        println!();
                    }
                }
                let _ = book_update_stream.disconnect().await;
            },
            Err(error) => println!("Error {:?}", error),
        }
    });
}
