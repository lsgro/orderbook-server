use log::{info, error};
use futures::prelude::*;
use rust_decimal::prelude::*;
use tokio::runtime::Runtime;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde_json::{Value, Map};
use serde::{Deserialize, Serialize};

use crate::core::*;


const BINANCE_CODE: &'static str = "binance";


pub struct BinanceProvider {
    product: CurrencyPair,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BinanceBookUpdate {
    lastUpdateId: u64,
    bids: Vec<(String, String)>,
    asks: Vec<(String, String)>,
}

impl From<&(String, String)> for Level {
    fn from(value: &(String, String)) -> Self {
        Level {
            exchange: BINANCE_CODE,
            price: Decimal::from_str(&value.0).unwrap(),
            amount: Decimal::from_str(&value.1).unwrap(),
        }
    }
}

impl From<BinanceBookUpdate> for BookUpdate {
    fn from(value: BinanceBookUpdate) -> Self {
        BookUpdate {
            exchange: BINANCE_CODE,
            timestamp: value.lastUpdateId,
            bids: value.bids.iter().map(|pair| pair.into()).collect(),
            asks: value.bids.iter().map(|pair| pair.into()).collect(),
        }
    }
}

impl BinanceProvider {
    pub fn new(product: CurrencyPair) -> BinanceProvider {
        BinanceProvider{ product }
    }

    pub async fn connect(&mut self) -> Result<BookStream, ProviderError> {
        let product_code = self.product.to_string().to_lowercase();
        let channel_code = format!("{}@depth{}@1000ms", product_code, NUM_LEVELS);
        let ws_url = format!("wss://stream.binance.com:443/ws/{}", channel_code);
        info!("Connecting to {}.", ws_url);
        match connect_async(ws_url).await {
            Ok((mut ws_stream, response)) => {
                info!("Connection {:?}", response);
                let subs_str = format!(r#"{{"method":"SUBSCRIBE","params":["{}"],"id":10}}"#, channel_code);
                info!("Subscribe to channel {}.", &subs_str);
                match ws_stream.send(Message::Text(subs_str)).await {
                    Ok(_) => {
                        match ws_stream.next().await {
                            Some(Ok(Message::Text(ref first_message_text))) => {
                                let first_message_json: Value = serde_json::from_str(first_message_text).unwrap();
                                let first_message_object: &Map<String, Value> = first_message_json.as_object().unwrap();
                                if first_message_object.contains_key("error") {
                                    Err(ProviderError::Subscription)
                                } else {
                                    info!("Subscription to channel succeeded.");
                                    let book_updates = ws_stream.filter_map(BinanceProvider::process_update);
                                    Ok(BookStream(Box::pin(book_updates)))
                                }
                            },
                            _ => Err(ProviderError::Subscription)
                        }
                    },
                    _ => Err(ProviderError::Io)
                }
            },
            _ => {
                Err(ProviderError::Io)
            }
        }
    }

    pub async fn disconnect(&mut self) {

    }

    async fn process_update(update_res: Result<Message, impl std::error::Error>) -> Option<BookUpdate> {
       match update_res {
           Ok(Message::Text(ref message_text)) => {
               match BinanceProvider::parse_book_update(message_text) {
                   Ok(book_update) => Some(book_update),
                   Err(err) => {
                       error!("Parse failed {:?}", err);
                       None
                   }
               }
           },
           _ => {
               error!("Receive failed {:?}", update_res);
               None
           }
       }
    }

    fn parse_book_update(message: &str) -> Result<BookUpdate, ProviderError> {
        let parse_res: serde_json::Result<BinanceBookUpdate> = serde_json::from_str(message);
        match parse_res {
            Ok(binance_book_update) => Ok(binance_book_update.into()),
            Err(_) => Err(ProviderError::Parse)
        }
    }
}
