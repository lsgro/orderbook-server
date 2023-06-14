//! Top level service consuming a stream of trading book snapshots, consolidating
//! them in an aggregate trading book and delivering snapshots of the
//! aggregate book via an output [stream](Stream).

use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::Stream;
use rust_decimal::prelude::ToPrimitive;

use crate::core::*;
use crate::aggregator::AggregateBook;
use crate::exchange::ExchangeDataStream;

use crate::orderbook::{Summary, Level};

/// Conversion from internal exchange price level to protobuf type.
impl From<&ExchangeLevel> for Level {
    fn from(value: &ExchangeLevel) -> Self {
        Level {
            exchange: value.exchange_code.to_string(),
            price: value.price.to_f64().unwrap(),
            amount: value.amount.to_f64().unwrap(),
        }
    }
}

/// Service providing a stream a consolidated book snapshots, one for each update
/// received from `book_update_stream`.
pub struct BookSummaryService {
    /// An object representing a merged stream of trading book snapshots.
    book_update_stream: Pin<Box<ExchangeDataStream<BookUpdate>>>,
    /// The aggregate book where all the trading book snapshots are consolidated.
    aggregate_book: AggregateBook,
}

impl  BookSummaryService {
    /// Create a new instance of the service.
    ///
    /// # Arguments
    ///
    /// * `book_update_stream` - An object of type [BookUpdateStream](ExchangeDataStream).
    ///
    /// # Returns
    ///
    /// An instance of [BookSummaryService](BookSummaryService)
    pub fn new(book_update_stream: ExchangeDataStream<BookUpdate>) -> Self {
        let aggregate_book = AggregateBook::new(NUM_LEVELS);
        Self { book_update_stream: Box::pin(book_update_stream), aggregate_book }
    }

    /// Disconnect from all exchanges, it consumes the service.
    pub async fn disconnect(self) {
        let book_update_stream: Box<ExchangeDataStream<BookUpdate>> = Pin::into_inner(self.book_update_stream);
        book_update_stream.disconnect().await;
    }

    /// Extract a protobuf message [Summary](Summary) from the current state of an aggregate book (static method).
    ///
    /// # Arguments
    ///
    /// * `aggregate_book` - A reference to an [aggregate book](AggregateBook).
    ///
    /// # Returns
    ///
    /// An instance of [Summary](Summary) object.
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

    /// Apply a [book update](BookUpdate) object if available, and return an up-to-date [Summary](Summary) object.
    ///
    /// # Arguments
    ///
    /// * `maybe_book_update` - An optional [BookUpdate](BookUpdate)
    ///
    /// # Returns
    ///
    /// An instance of [Summary](Summary) object.
    fn update_and_make_summary(&mut self, maybe_book_update: Option<BookUpdate>) -> Summary {
        if let Some(book_update) = maybe_book_update {
            self.aggregate_book.update(book_update);
        }
        Self::make_summary(&self.aggregate_book)
    }
}

/// [Stream](Stream) implementation for the service producing protobuf [Summary](Summary) objects.
impl  Stream for BookSummaryService {
    type Item = Summary;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.book_update_stream.as_mut().poll_next(cx).map( |maybe_book_update|
            Some(self.update_and_make_summary(maybe_book_update))
        )
    }
}

