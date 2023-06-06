use futures::prelude::*;
use crate::core::*;


const BITSTAMP_CODE: &'static str = "bitstamp";


struct BitstampProvider {
    product: CurrencyPair,
}

impl BitstampProvider {
    pub fn new(product: CurrencyPair) -> BitstampProvider {
        BitstampProvider{product}
    }
}
