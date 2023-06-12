//! Base data structures.

use std::fmt::{Display, Formatter};
use rust_decimal::prelude::*;

/// Default number of levels for each side of the consolidated trading book.
pub const NUM_LEVELS: usize = 10;


/// Trading book side indicator
#[derive(PartialEq, Debug)]
pub enum Side {
    Buy,
    Sell,
}

/// The product traded: a currency pair
#[derive(PartialEq, Debug, Clone)]
pub struct CurrencyPair {
    pub main: String,
    pub counter: String,
}

impl Display for CurrencyPair {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.main, self.counter)
    }
}

/// Part of a trading book snapshot received from an exchange.
/// This object represents a single price level belonging to a side of the book (bid/ask).
#[derive(PartialEq, Debug)]
pub struct ExchangeLevel {
    /// Exchange code
    pub exchange_code: &'static str,
    /// Level price
    pub price: Decimal,
    /// Amount available on the exchange's book
    pub amount: Decimal,
}

impl ExchangeLevel {
    /// Utility function to create an [ExchangeLevel](ExchangeLevel) object from string values.
    pub fn from_strs(exchange_code: &'static str, price_str: &str, amount_str: &str) -> ExchangeLevel {
        ExchangeLevel {
            exchange_code,
            price: Decimal::from_str(price_str).unwrap(),
            amount: Decimal::from_str(amount_str).unwrap(),
        }
    }
}

/// A trading book snapshot from an exchange.
#[derive(PartialEq, Debug)]
pub struct BookUpdate {
    /// Exchange code
    pub exchange_code: &'static str,
    /// Bid levels
    pub bids: Vec<ExchangeLevel>,
    /// Ask levels
    pub asks: Vec<ExchangeLevel>,
}
