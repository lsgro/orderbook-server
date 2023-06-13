//! Binance WebSocket exchange adapter for periodic trading book snapshots.

use log::debug;
use rust_decimal::prelude::*;
use serde::{Deserialize};

use crate::core::*;
use crate::exchange::BookUpdateSource;


const BINANCE_CODE: &str = "binance";
const BINANCE_WS_URL: &str = "wss://stream.binance.com:443/ws";


fn read_binance_book_update(value: &str) -> Option<BookUpdate> {
    let parse_res: serde_json::Result<BinanceBookUpdate> = serde_json::from_str(value);
    match parse_res {
        Ok(book_update @ BinanceBookUpdate{..}) => Some(book_update.into()),
        _ => {
            debug!("Parse failed {:?}", value);
            None
        }
    }
}

pub async fn make_binance_book_update_source(product: &CurrencyPair) -> BookUpdateSource {
    let product_code = product.to_string().to_lowercase();
    let channel_code = format!("{}@depth{}@100ms", product_code, NUM_LEVELS);
    let ws_url = format!("{}/{}", BINANCE_WS_URL, channel_code);
    let subscribe_message = format!(r#"{{"method":"SUBSCRIBE","params":["{}"],"id":10}}"#, channel_code);
    BookUpdateSource::new(
        BINANCE_CODE,
        ws_url,
        subscribe_message,
        &read_binance_book_update,
    ).await
}

#[derive(Deserialize, Debug)]
struct BinancePair((String, String));

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct BinanceBookUpdate {
    bids: Vec<BinancePair>,
    asks: Vec<BinancePair>,
}

impl From<BinancePair> for ExchangeLevel {
    fn from(value: BinancePair) -> Self {
        let BinancePair((price_str, amount_str)) = value;
        Self {
            exchange_code: BINANCE_CODE,
            price: Decimal::from_str(&price_str).unwrap(),
            amount: Decimal::from_str(&amount_str).unwrap(),
        }
    }
}

impl From<BinanceBookUpdate> for BookUpdate {
    fn from(value: BinanceBookUpdate) -> Self {
        Self {
            exchange_code: BINANCE_CODE,
            bids: value.bids.into_iter().map(|pair| pair.into()).collect(),
            asks: value.asks.into_iter().map(|pair| pair.into()).collect(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_binance_book_update() {
        let b_book_update = BinanceBookUpdate {
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
            exchange_code: BINANCE_CODE,
            bids: vec![
                ExchangeLevel::from_strs(BINANCE_CODE, "0.123", "123.1"),
                ExchangeLevel::from_strs(BINANCE_CODE, "0.321", "321.3"),
            ],
            asks: vec![
                ExchangeLevel::from_strs(BINANCE_CODE, "3.213", "321.3"),
                ExchangeLevel::from_strs(BINANCE_CODE, "1.231", "122.1"),
            ],
        };
        let book_update: BookUpdate = b_book_update.into();
        assert_eq!(book_update, exp_book_update);
    }
}