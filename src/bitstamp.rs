//! Bitstamp WebSocket exchange adapter for trading book snapshots.

use log::debug;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use crate::core::*;
use crate::exchange::{BookUpdateReader, BookUpdateSource};


const BITSTAMP_CODE: &str = "bitstamp";
const BITSTAMP_WS_URL: &str = "wss://ws.bitstamp.net";


/// Bitstamp implementation of the message parser [BookUpdateReader](BookUpdateReader).
struct BitstampBookUpdateReader;

impl BookUpdateReader for BitstampBookUpdateReader {
    fn read_book_update(&self, value: String) -> Option<BookUpdate> {
        let parse_res: serde_json::Result<BitstampBookUpdate> = serde_json::from_str(&value);
        match parse_res {
            Ok(book_update @ BitstampBookUpdate{..}) => Some(book_update.into()),
            _ => {
                debug!("Parse failed {:?}", &value);
                None
            }
        }
    }
}

/// Bitstamp implementation of the exchange adapter [BookUpdateSource](BookUpdateSource).
pub struct BitstampBookUpdateSource {
    ws_url: String,
    subscribe_msg: String,
}

impl BitstampBookUpdateSource {
    pub fn new(product: &CurrencyPair) -> Self {
        let product_code = product.to_string().to_lowercase();
        let channel_code = format!("order_book_{}", product_code);
        let ws_url = String::from(BITSTAMP_WS_URL);
        let subscribe_msg = format!(r#"{{"event": "bts:subscribe","data":{{"channel":"{}"}}}}"#, channel_code);
        Self { ws_url, subscribe_msg }
    }
}

impl BookUpdateSource for BitstampBookUpdateSource {
    fn ws_url(&self) -> String {
        self.ws_url.clone()
    }

    fn subscribe_message(&self) -> String {
        self.subscribe_msg.clone()
    }

    fn make_book_update_reader(&self) ->  Box<dyn BookUpdateReader> {
        Box::new(BitstampBookUpdateReader)
    }

    fn exchange_code(&self) -> &'static str {
        BITSTAMP_CODE
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct BitstampPair((String, String));

#[derive(Serialize, Deserialize, Debug)]
struct BitstampBookUpdateData {
    timestamp: String,
    microtimestamp: String,
    bids: Vec<BitstampPair>,
    asks: Vec<BitstampPair>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BitstampBookUpdate {
    data: BitstampBookUpdateData,
    channel: String,
    event: String,
}

impl From<BitstampPair> for ExchangeLevel {
    fn from(value: BitstampPair) -> Self {
        let BitstampPair((price_str, amount_str)) = value;
        Self {
            exchange_code: BITSTAMP_CODE,
            price: Decimal::from_str(&price_str).unwrap(),
            amount: Decimal::from_str(&amount_str).unwrap(),
        }
    }
}

impl From<BitstampBookUpdate> for BookUpdate {
    fn from(value: BitstampBookUpdate) -> Self {
        Self {
            exchange_code: BITSTAMP_CODE,
            bids: value.data.bids.into_iter().take(NUM_LEVELS).map(|pair| pair.into()).collect(),
            asks: value.data.asks.into_iter().take(NUM_LEVELS).map(|pair| pair.into()).collect(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_bitstamp_book_update() {
        let b_book_update = BitstampBookUpdate {
            data: BitstampBookUpdateData {
                timestamp: "123".to_string(),
                microtimestamp: "123000".to_string(),
                bids: vec![
                    BitstampPair(("0.123".to_string(), "123.1".to_string())),
                    BitstampPair(("0.321".to_string(), "321.3".to_string()))
                ],
                asks: vec![
                    BitstampPair(("3.213".to_string(), "321.3".to_string())),
                    BitstampPair(("1.231".to_string(), "122.1".to_string()))
                ],
            },
            channel: "test channel".to_string(),
            event: "data".to_string(),
        };
        let exp_book_update = BookUpdate {
            exchange_code: BITSTAMP_CODE,
            bids: vec![
                ExchangeLevel::from_strs(BITSTAMP_CODE, "0.123", "123.1"),
                ExchangeLevel::from_strs(BITSTAMP_CODE, "0.321", "321.3"),
            ],
            asks: vec![
                ExchangeLevel::from_strs(BITSTAMP_CODE, "3.213", "321.3"),
                ExchangeLevel::from_strs(BITSTAMP_CODE, "1.231", "122.1"),
            ],
        };
        let book_update: BookUpdate = b_book_update.into();
        assert_eq!(book_update, exp_book_update);
    }
}