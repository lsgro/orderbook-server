use std::pin::Pin;
use futures::stream::{Stream, select, StreamExt};
use rust_decimal::prelude::ToPrimitive;

use crate::core::*;
use crate::bitstamp::make_bitstamp_provider;
use crate::binance::make_binance_provider;
use crate::aggregator::AggregateBook;

use crate::orderbook::{Summary, Level};


impl From<&ExchangeLevel> for Level {
    fn from(value: &ExchangeLevel) -> Self {
        Level {
            exchange: value.exchange.to_string(),
            price: value.price.to_f64().unwrap(),
            amount: value.amount.to_f64().unwrap(),
        }
    }
}

impl From<&AggregateBook> for Summary {
    fn from(value: &AggregateBook) -> Self {
        let best_bids = value.best_bids();
        let best_asks = value.best_asks();
        let bids: Vec<Level> = best_bids.iter().map(|&l| l.into()).collect();
        let asks: Vec<Level> = best_asks.iter().map(|&l| l.into()).collect();
        let spread = if best_bids.is_empty() || best_asks.is_empty() {
            f64::NAN
        } else {
            (best_asks[0].price - best_bids[0].price).to_f64().unwrap()
        };
        Summary { spread, bids, asks }
    }
}

pub struct BookSummaryService {
    stream: Pin<Box<dyn Stream<Item = Summary> + Send>>,
}

impl BookSummaryService {
    pub async fn new(product: CurrencyPair) -> Self {
        let bitstamp_provider = make_bitstamp_provider(&product);
        let binance_provider = make_binance_provider(&product);
        let connected_bitstamp_provider = bitstamp_provider.connect().await.expect("Connection to Bitstamp failed");
        let connected_binance_provider = binance_provider.connect().await.expect("Connection to Binance failed");
        let mut aggregate_book = AggregateBook::new(NUM_LEVELS);
        let stream= Box::pin(select(connected_bitstamp_provider, connected_binance_provider).map(
            move |book_update| {
                aggregate_book.update(book_update);
                let summary: Summary = (&aggregate_book).into();
                summary
            }
        ));
        Self { stream }
    }
}

impl From<BookSummaryService> for Pin<Box<dyn Stream<Item = Summary> + Send>> {
    fn from(value: BookSummaryService) -> Self {
        Box::pin(value.stream)
    }
}