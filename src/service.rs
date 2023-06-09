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
    book_update_stream: Pin<Box<Select<ConnectedBookUpdateProvider, ConnectedBookUpdateProvider>>>,
    aggregate_book: AggregateBook,
}

impl BookSummaryService {
    pub async fn new(product: CurrencyPair) -> Self {
        let bitstamp_provider = make_bitstamp_provider(&product);
        let binance_provider = make_binance_provider(&product);
        let connected_bitstamp_provider = bitstamp_provider.connect().await.expect("Connection to Bitstamp failed");
        let connected_binance_provider = binance_provider.connect().await.expect("Connection to Binance failed");
        let book_update_stream= Box::pin(select(connected_bitstamp_provider, connected_binance_provider));
        let aggregate_book = AggregateBook::new(NUM_LEVELS);
        Self { book_update_stream, aggregate_book }
    }

    pub async fn disconnect(self) {
        let (stream1, stream2) = Pin::into_inner(self.book_update_stream).into_inner();
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

    fn update_and_make_summary(&mut self, maybe_book_update: Option<BookUpdate>) -> Summary {
        if let Some(book_update) = maybe_book_update {
            self.aggregate_book.update(book_update);
        }
        Self::make_summary(&self.aggregate_book)
    }
}

impl Stream for BookSummaryService {
    type Item = Summary;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.book_update_stream.as_mut().poll_next(cx).map( |maybe_book_update|
            Some(self.update_and_make_summary(maybe_book_update))
        )
    }
}
