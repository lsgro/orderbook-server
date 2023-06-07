use std::fmt::{Display, Formatter};
use rust_decimal::prelude::*;


pub const NUM_LEVELS: usize = 10;


#[derive(PartialEq, Debug)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(PartialEq, Debug)]
pub struct CurrencyPair {
    pub main: &'static str,
    pub counter: &'static str,
}

impl Display for CurrencyPair {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.main, self.counter)
    }
}

#[derive(Debug)]
pub struct Level {
    pub exchange: &'static str,
    pub price: Decimal,
    pub amount: Decimal,
}

#[derive(Debug)]
pub struct BookUpdate {
    pub exchange: &'static str,
    pub timestamp: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Debug)]
pub struct Summary {
    pub spread: Decimal,
    pub bids: [Level; NUM_LEVELS],
    pub asks: [Level; NUM_LEVELS],
}

#[derive(Debug)]
pub enum ProviderError {
    Http,
    Io,
    Subscription,
    Parse,
}

