pub mod core;
pub mod binance;
pub mod bitstamp;
pub mod exchange;
pub mod aggregator;
pub mod service;
pub mod orderbook {
    tonic::include_proto!("orderbook");
}


