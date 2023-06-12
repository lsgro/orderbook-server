use std::fmt::{Display, Formatter};
use rust_decimal::prelude::*;


pub const NUM_LEVELS: usize = 10;


#[derive(PartialEq, Debug)]
pub enum Side {
    Buy,
    Sell,
}

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

#[derive(PartialEq, Debug)]
pub struct ExchangeLevel {
    pub exchange: &'static str,
    pub price: Decimal,
    pub amount: Decimal,
}

impl ExchangeLevel {
    pub fn from_strs(exchange: &'static str, price_str: &str, amount_str: &str) -> ExchangeLevel {
        ExchangeLevel {
            exchange,
            price: Decimal::from_str(price_str).unwrap(),
            amount: Decimal::from_str(amount_str).unwrap(),
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct BookUpdate {
    pub exchange: &'static str,
    pub bids: Vec<ExchangeLevel>,
    pub asks: Vec<ExchangeLevel>,
}

#[derive(Debug)]
pub enum ProviderError {
    Http,
    Io,
    Subscription,
    Parse,
}

