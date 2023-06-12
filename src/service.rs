use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::Stream;
use rust_decimal::prelude::ToPrimitive;

use crate::core::*;
use crate::aggregator::AggregateBook;
use crate::exchange::BookUpdateStream;

use crate::orderbook::{Summary, Level};

/// Conversion between internal exchange price level to protobuf type.
impl From<&ExchangeLevel> for Level {
    fn from(value: &ExchangeLevel) -> Self {
        Level {
            exchange: value.exchange.to_string(),
            price: value.price.to_f64().unwrap(),
            amount: value.amount.to_f64().unwrap(),
        }
    }
}

/// Service connecting two Binance and Bitstamp to stream a consolidated book
pub struct BookSummaryService {
    /// A merged stream of Binance and Bitmap trading book snapshots.
    ///
    /// # Returns
    ///
    /// A stream of [BookUpdate](BookUpdate) objects
    book_update_stream: Pin<Box<BookUpdateStream>>,
    aggregate_book: AggregateBook,
}

impl BookSummaryService {
    /// Return a new instance of the service.
    ///
    /// # Arguments
    ///
    /// * `book_update_stream` - A stream of [BookUpdate](BookUpdate)
    ///
    /// # Returns
    ///
    /// An instance of [BookSummaryService](BookSummaryService)
    pub fn new(book_update_stream: BookUpdateStream) -> Self {
        let aggregate_book = AggregateBook::new(NUM_LEVELS);
        Self { book_update_stream: Box::pin(book_update_stream), aggregate_book }
    }

    /// Disconnect from all exchanges, it consumes the service
    pub async fn disconnect(self) {
        let book_update_stream: Box<BookUpdateStream> = Pin::into_inner(self.book_update_stream);
        book_update_stream.disconnect().await;
    }

    /// Extract a protobuf message [Summary](Summary) from the current state of an aggregate book (static method).
    ///
    /// # Arguments
    ///
    /// * `aggregate_book` - A reference to an (aggregate book)[AggregateBook]
    ///
    /// # Returns
    ///
    /// An instance of [BookSummaryService](BookSummaryService)
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
    /// * `product` - An optional [BookUpdate](BookUpdate)
    ///
    /// # Return
    fn update_and_make_summary(&mut self, maybe_book_update: Option<BookUpdate>) -> Summary {
        if let Some(book_update) = maybe_book_update {
            self.aggregate_book.update(book_update);
        }
        Self::make_summary(&self.aggregate_book)
    }
}

/// (Stream)[Stream] implementation for the service that outputs protobuf (Summary)[Summary] objects.
impl Stream for BookSummaryService {
    type Item = Summary;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.book_update_stream.as_mut().poll_next(cx).map( |maybe_book_update|
            Some(self.update_and_make_summary(maybe_book_update))
        )
    }
}

