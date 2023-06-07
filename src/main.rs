use tokio::runtime::Runtime;
use log::LevelFilter;
use simple_logger::SimpleLogger;
use futures::stream;

use keyrock_eu_lsgro::core::*;
use keyrock_eu_lsgro::bitstamp::make_bitstamp_provider;
use keyrock_eu_lsgro::binance::make_binance_provider;

//tmp
use futures::prelude::*;

fn main() {
    SimpleLogger::new().with_level(LevelFilter::Info).init().unwrap();
    let rt = Runtime::new().unwrap();
    let product = CurrencyPair { main: "ETH", counter: "BTC" };
    let bitstamp_provider = make_bitstamp_provider(&product);
    let binance_provider = make_binance_provider(&product);
    rt.block_on(async {
        let connected_bitstamp_provider = bitstamp_provider.connect().await.unwrap();
        let connected_binance_provider = binance_provider.connect().await.unwrap();
        let mut merged_provider = stream::select(connected_bitstamp_provider, connected_binance_provider);
        for _ in 0..1000 {
            merged_provider.next().await.map(|book_update| println!("{:?}", book_update));
        }
        let (stream1, stream2) = merged_provider.into_inner();
        let _ = stream1.disconnect().await;
        let _ = stream2.disconnect().await;
    });
}
