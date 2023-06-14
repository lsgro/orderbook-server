//! Service listening to trading book updates from multiple
//! exchanges, and publishing snapshots for a consolidate trading book
//! as Protobuf service.

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


