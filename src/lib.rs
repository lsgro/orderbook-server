//! This crate provides a `gRPC` service attaching to multiple
//! exchanges, listening to concurrent trading book updates,
//! and publishing snapshots for a consolidate trading book.
//! Example client implementation provided in `src/client.rs`.

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


