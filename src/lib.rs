pub mod core;
mod aggregator;
pub mod exchange;
pub mod binance;
pub mod bitstamp;
pub mod service;
pub mod cli;

pub mod orderbook {
    tonic::include_proto!("orderbook");
}


