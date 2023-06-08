use log::debug;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use crate::core::*;
use crate::exchange::{BookUpdateReader, BookUpdateProvider};


const BITSTAMP_CODE: &'static str = "bitstamp";
const BITSTAMP_WS_URL: &'static str = "wss://ws.bitstamp.net";


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
        ExchangeLevel {
            exchange: BITSTAMP_CODE,
            price: Decimal::from_str(&price_str).unwrap(),
            amount: Decimal::from_str(&amount_str).unwrap(),
        }
    }
}

impl From<BitstampBookUpdate> for BookUpdate {
    fn from(value: BitstampBookUpdate) -> Self {
        BookUpdate {
            exchange: BITSTAMP_CODE,
            bids: value.data.bids.into_iter().take(NUM_LEVELS).map(|pair| pair.into()).collect(),
            asks: value.data.asks.into_iter().take(NUM_LEVELS).map(|pair| pair.into()).collect(),
        }
    }
}

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

pub fn make_bitstamp_provider(product: &CurrencyPair) -> BookUpdateProvider {
    let product_code = product.to_string().to_lowercase();
    let channel_code = format!("order_book_{}", product_code);
    let ws_url = String::from(BITSTAMP_WS_URL);
    let subscribe_msg = format!(r#"{{"event": "bts:subscribe","data":{{"channel":"{}"}}}}"#, channel_code);
    let book_update_reader = Box::new(BitstampBookUpdateReader);
    BookUpdateProvider::new(ws_url, subscribe_msg, book_update_reader)
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
            exchange: BITSTAMP_CODE,
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