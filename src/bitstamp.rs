//! Bitstamp `WebSocket` exchange adapter for trading book snapshots.

use log::debug;
use rust_decimal::prelude::*;
use serde::{Deserialize};

use crate::core::*;
use crate::exchange::{ExchangeAdapter, ExchangeProtocol};


const BITSTAMP_CODE: &str = "bitstamp";
const BITSTAMP_WS_URL: &str = "wss://ws.bitstamp.net";

/// Parse string messages from trading book update Bitstamp WebSocket service into
/// the exchange [protocol](ExchangeProtocol).
/// It recognizes trading book updates and reconnection requests.
fn read_bitstamp_book_update(value: &str) -> Option<ExchangeProtocol<BookUpdate>> {
    let data_result: serde_json::Result<BitstampBookUpdate> = serde_json::from_str(&value);
    match data_result {
        Ok(book_update @ BitstampBookUpdate {..}) => {
            Some(ExchangeProtocol::Data(book_update.into()))
        },
        _ => {
            let event_result: serde_json::Result<BitstampEvent> = serde_json::from_str(&value);
            if let Ok(BitstampEvent {event}) = event_result {
                if event == "bts:request_reconnect" {
                    Some(ExchangeProtocol::ReconnectionRequest)
                } else {
                    debug!("Event not recognized: {}", event);
                    None
                }
            } else {
                debug!("Parse failed {:?}", &value);
                None
            }
        }
    }
}

/// Creates an [exchange adapter](ExchangeAdapter) for Bitstamp.
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

#[derive(Deserialize, Debug)]
struct BitstampEvent {
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
    fn test_read_bitstamp_book_update_success() {
        let websocket_msg = r#"{"data":{"timestamp":"1686727555","microtimestamp":"1686727555138288","bids":[["0.00001041","9076.13940234"],["0.00001040","9994.00000000"]],"asks":[["0.00001046","27295.53635305"],["0.00001102","73663.12239490"]]},"channel":"order_book_adabtc","event":"data"}"#;
        let parsed = read_bitstamp_book_update(websocket_msg);
        let expected = Some(ExchangeProtocol::Data(BookUpdate{
            exchange_code: "bitstamp",
            bids: vec![
                ExchangeLevel::from_strs("bitstamp", "0.00001041","9076.13940234"),
                ExchangeLevel::from_strs("bitstamp", "0.00001040","9994.00000000")
            ],
            asks: vec![
                ExchangeLevel::from_strs("bitstamp", "0.00001046","27295.53635305"),
                ExchangeLevel::from_strs("bitstamp", "0.00001102","73663.12239490"),
            ],
        }));
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_read_bitstamp_reconnect_success() {
        let websocket_msg = r#"{"event":"bts:request_reconnect","channel":"","data":"" }"#;
        let parsed = read_bitstamp_book_update(websocket_msg);
        let expected = Some(ExchangeProtocol::ReconnectionRequest);
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_read_bitstamp_book_update_failure() {
        let websocket_msg = r#"{"lastUpdateId":1580041371,"bids":[["0.00001049","9383.30000000"],["__INCORRECT__"]],"asks":[["0.00001050","133639.50000000"],["0.00001051","133083.10000000"]]}"#;
        let parsed = read_bitstamp_book_update(websocket_msg);
        assert_eq!(parsed, None);
    }

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