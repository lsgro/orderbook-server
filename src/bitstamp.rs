//! Bitstamp WebSocket exchange adapter for trading book snapshots.

use log::debug;
use rust_decimal::prelude::*;
use serde::{Deserialize};

use crate::core::*;
use crate::exchange::{ExchangeAdapter, ExchangeProtocol};


const BITSTAMP_CODE: &str = "bitstamp";
const BITSTAMP_WS_URL: &str = "wss://ws.bitstamp.net";


fn read_bitstamp_book_update(value: &str) -> Option<ExchangeProtocol<BookUpdate>> {
    let parse_res: serde_json::Result<BitstampBookUpdate> = serde_json::from_str(&value);
    match parse_res {
        Ok(book_update @ BitstampBookUpdate{..}) => {
            Some(ExchangeProtocol::Data(book_update.into()))
        },
        _ => {
            debug!("Parse failed {:?}", &value);
            None
        }
    }
}

pub async fn make_bitstamp_echange_adapter(product: &CurrencyPair) -> ExchangeAdapter<BookUpdate> {
    let product_code = product.to_string().to_lowercase();
    let channel_code = format!("order_book_{}", product_code);
    let ws_url = String::from(BITSTAMP_WS_URL);
    let subscribe_message = format!(r#"{{"event": "bts:subscribe","data":{{"channel":"{}"}}}}"#, channel_code);
    ExchangeAdapter::new(
        BITSTAMP_CODE,
        ws_url,
        subscribe_message,
        &read_bitstamp_book_update,
    ).await
}

#[derive(Deserialize, Debug)]
struct BitstampPair((String, String));

#[derive(Deserialize, Debug)]
struct BitstampBookUpdateData {
    bids: Vec<BitstampPair>,
    asks: Vec<BitstampPair>,
}

#[derive(Deserialize, Debug)]
struct BitstampBookUpdate {
    data: BitstampBookUpdateData,
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
                bids: vec![
                    BitstampPair(("0.123".to_string(), "123.1".to_string())),
                    BitstampPair(("0.321".to_string(), "321.3".to_string()))
                ],
                asks: vec![
                    BitstampPair(("3.213".to_string(), "321.3".to_string())),
                    BitstampPair(("1.231".to_string(), "122.1".to_string()))
                ],
            },
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