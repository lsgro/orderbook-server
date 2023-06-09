use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::{Stream, select, Select};
use rust_decimal::prelude::ToPrimitive;

use crate::core::*;
use crate::bitstamp::make_bitstamp_provider;
use crate::binance::make_binance_provider;
use crate::aggregator::AggregateBook;
use crate::exchange::ConnectedBookUpdateProvider;

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

pub struct BookSummaryService {
    aggregate_book: AggregateBook,
    stream: Pin<Box<Select<ConnectedBookUpdateProvider, ConnectedBookUpdateProvider>>>,
}

impl BookSummaryService {
    pub async fn new(product: CurrencyPair) -> Self {
        let bitstamp_provider = make_bitstamp_provider(&product);
        let binance_provider = make_binance_provider(&product);
        let connected_bitstamp_provider = bitstamp_provider.connect().await.expect("Connection to Bitstamp failed");
        let connected_binance_provider = binance_provider.connect().await.expect("Connection to Binance failed");
        let stream= Box::pin(select(connected_bitstamp_provider, connected_binance_provider));
        let aggregate_book = AggregateBook::new(NUM_LEVELS);
        Self { aggregate_book, stream }
    }

    pub async fn disconnect(self) {
        let (stream1, stream2) = Pin::into_inner(self.stream).into_inner();
        let _ = stream1.disconnect().await;
        let _ = stream2.disconnect().await;
    }

    fn make_summary(aggregate_book: &AggregateBook) -> Summary {
        let best_bids = aggregate_book.best_bids();
        let best_asks = aggregate_book.best_asks();
        let bids: Vec<Level> = best_bids.iter().map(|&l| l.into()).collect();
        let asks: Vec<Level> = best_asks.iter().map(|&l| l.into()).collect();
        let spread = if best_bids.is_empty() || best_asks.is_empty() {
            f64::NAN
        } else {
            (best_asks[0].price - best_bids[0].price).to_f64().unwrap_or(f64::NAN)
        };
        Summary { spread, bids, asks }
    }
}

impl Stream for BookSummaryService {
    type Item = Summary;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx).map(
            move |maybe_book_update| {
                if let Some(book_update) = maybe_book_update {
                    self.aggregate_book.update(book_update);
                }
                let summary: Summary = Self::make_summary(&self.aggregate_book);
                Some(summary)
            }
        )
    }
}
