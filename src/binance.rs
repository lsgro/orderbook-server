use log::debug;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

use crate::core::*;
use crate::book_ws::{BookUpdateReader, BookUpdateProvider};


const BINANCE_CODE: &'static str = "binance";
const BINANCE_WS_URL: &'static str = "wss://stream.binance.com:443/ws";


#[derive(Serialize, Deserialize, Debug)]
struct BinancePair((String, String));

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BinanceBookUpdate {
    last_update_id: u64,
    bids: Vec<BinancePair>,
    asks: Vec<BinancePair>,
}

impl From<BinancePair> for Level {
    fn from(value: BinancePair) -> Self {
        let BinancePair((price_str, amount_str)) = value;
        Level {
            exchange: BINANCE_CODE,
            price: Decimal::from_str(&price_str).unwrap(),
            amount: Decimal::from_str(&amount_str).unwrap(),
        }
    }
}

impl From<BinanceBookUpdate> for BookUpdate {
    fn from(value: BinanceBookUpdate) -> Self {
        BookUpdate {
            exchange: BINANCE_CODE,
            bids: value.bids.into_iter().map(|pair| pair.into()).collect(),
            asks: value.asks.into_iter().map(|pair| pair.into()).collect(),
        }
    }
}

struct BinanceBookUpdateReader;

impl BookUpdateReader for BinanceBookUpdateReader {
    fn read_book_update(&self, value: String) -> Option<BookUpdate> {
        let parse_res: serde_json::Result<BinanceBookUpdate> = serde_json::from_str(&value);
        match parse_res {
            Ok(book_update @ BinanceBookUpdate{..}) => Some(book_update.into()),
            _ => {
                debug!("Parse failed {:?}", &value);
                None
            }
        }
    }
}

pub fn make_binance_provider(product: &CurrencyPair) -> BookUpdateProvider {
    let product_code = product.to_string().to_lowercase();
    let channel_code = format!("{}@depth{}@100ms", product_code, NUM_LEVELS);
    let ws_url = format!("{}/{}", BINANCE_WS_URL, channel_code);
    let subscribe_msg = format!(r#"{{"method":"SUBSCRIBE","params":["{}"],"id":10}}"#, channel_code);
    let book_update_reader = Box::new(BinanceBookUpdateReader);
    BookUpdateProvider::new(ws_url, subscribe_msg, book_update_reader)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_binance_book_update() {
        let b_book_update = BinanceBookUpdate {
            last_update_id: 333,
            bids: vec![
                BinancePair(("0.123".to_string(), "123.1".to_string())),
                BinancePair(("0.321".to_string(), "321.3".to_string()))
            ],
            asks: vec![
                BinancePair(("3.213".to_string(), "321.3".to_string())),
                BinancePair(("1.231".to_string(), "122.1".to_string()))
            ],
        };
        let exp_book_update = BookUpdate {
            exchange: BINANCE_CODE,
            bids: vec![
                Level::from_strs(BINANCE_CODE, "0.123", "123.1"),
                Level::from_strs(BINANCE_CODE, "0.321", "321.3"),
            ],
            asks: vec![
                Level::from_strs(BINANCE_CODE, "3.213", "321.3"),
                Level::from_strs(BINANCE_CODE, "1.231", "122.1"),
            ],
        };
        let book_update: BookUpdate = b_book_update.into();
        assert_eq!(book_update, exp_book_update);
    }
}