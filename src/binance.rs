use log::{info, error};
use futures::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use rust_decimal::prelude::*;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream, tungstenite::protocol::Message};
use serde::{Deserialize, Serialize};

use crate::core::*;


const BINANCE_CODE: &'static str = "binance";

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
            timestamp: value.last_update_id,
            bids: value.bids.into_iter().map(|pair| pair.into()).collect(),
            asks: value.asks.into_iter().map(|pair| pair.into()).collect(),
        }
    }
}

pub struct BinanceProvider {
    product: CurrencyPair,
}

impl BinanceProvider {
    pub fn new(product: CurrencyPair) -> BinanceProvider {
        BinanceProvider{ product }
    }

    pub async fn connect(&mut self) -> Result<ConnectedBinanceProvider, ProviderError> {
        let product_code = self.product.to_string().to_lowercase();
        let channel_code = format!("{}@depth{}@1000ms", product_code, NUM_LEVELS);
        let ws_url = format!("wss://stream.binance.com:443/ws/{}", channel_code);
        info!("Connecting to {}.", ws_url);
        match connect_async(ws_url).await {
            Ok((mut ws, response)) => {
                info!("Connection {:?}", response);
                let subs_str = format!(r#"{{"method":"SUBSCRIBE","params":["{}"],"id":10}}"#, channel_code);
                info!("Subscribe to channel {}.", &subs_str);
                match ws.send(Message::Text(subs_str)).await {
                    Ok(_) => {
                        info!("Subscription to channel succeeded.");
                        Ok(ConnectedBinanceProvider{ ws_stream: Box::pin(ws) })
                    },
                    _ => Err(ProviderError::Io)
                }
            },
            _ => {
                Err(ProviderError::Io)
            }
        }
    }
}

pub struct ConnectedBinanceProvider {
    ws_stream: Pin<Box<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>,
}

impl ConnectedBinanceProvider {
    pub async fn disconnect(&mut self) -> Result<(), ProviderError>{
        info!("Disconnect socket");
        self.ws_stream.close().await.map_err(|_| ProviderError::Io)
    }

    fn parse_book_update(msg_txt: &str) -> Option<BookUpdate> {
        let parse_res: serde_json::Result<BinanceBookUpdate> = serde_json::from_str(msg_txt);
        match parse_res {
            Ok(book_update @ BinanceBookUpdate{..}) => Some(book_update.into()),
            _ => {
                error!("Parse failed {:?}", msg_txt);
                None
            }
        }
    }
}

impl Stream for ConnectedBinanceProvider {
    type Item = BookUpdate;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.ws_stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(Message::Text(ref msg_txt)))) => {
                let maybe_book_update = ConnectedBinanceProvider::parse_book_update(msg_txt);
                Poll::Ready(maybe_book_update)
            }
            Poll::Ready(Some(Ok(Message::Ping(data)))) => {
                info!("Ping received. Respond.");
                let _ = self.ws_stream.send(Message::Pong(data));
                Poll::Ready(None)
            }
            _ => Poll::Pending
        }
    }
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
            timestamp: 333,
            bids: vec![
                Level {
                    exchange: BINANCE_CODE,
                    price: Decimal::from_str("0.123").unwrap(),
                    amount: Decimal::from_str("123.1").unwrap(),
                },
                Level {
                    exchange: BINANCE_CODE,
                    price: Decimal::from_str("0.321").unwrap(),
                    amount: Decimal::from_str("321.3").unwrap(),
                },
            ],
            asks: vec![
                Level {
                    exchange: BINANCE_CODE,
                    price: Decimal::from_str("3.213").unwrap(),
                    amount: Decimal::from_str("321.3").unwrap(),
                },
                Level {
                    exchange: BINANCE_CODE,
                    price: Decimal::from_str("1.231").unwrap(),
                    amount: Decimal::from_str("122.1").unwrap(),
                },
            ],
        };
        let book_update: BookUpdate = b_book_update.into();
        assert_eq!(book_update, exp_book_update);
    }
}