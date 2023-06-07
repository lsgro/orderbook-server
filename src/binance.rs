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
#[serde(rename_all = "camelCase")]
pub struct BinanceBookUpdate {
    last_update_id: u64,
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
            timestamp: value.last_update_id,
            bids: value.bids.iter().map(|pair| pair.into()).collect(),
            asks: value.bids.iter().map(|pair| pair.into()).collect(),
        }
    }
}

pub struct BinanceProvider {
    product: CurrencyPair,
    ws_stream: Option<Pin<Box<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>>
}

impl BinanceProvider {
    pub fn new(product: CurrencyPair) -> BinanceProvider {
        BinanceProvider{ product, ws_stream: None }
    }

    pub async fn connect(&mut self) -> Result<(), ProviderError> {
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
                        self.ws_stream = Some(Box::pin(ws));
                        Ok(())
                    },
                    _ => Err(ProviderError::Io)
                }
            },
            _ => {
                Err(ProviderError::Io)
            }
        }
    }

    pub async fn disconnect(&mut self) -> Result<(), ProviderError>{
        info!("Disconnect socket");
        let mut ws = self.ws_stream.take().expect("WebSocket already closed");
        ws.close().await.map_err(|_| ProviderError::Io)
    }

    fn process_message(msg: Option<Result<Message, impl std::error::Error>>) -> Option<BookUpdate> {
       match msg {
           Some(Ok(Message::Text(ref msg_txt))) => {
               let parse_res: serde_json::Result<BinanceBookUpdate> = serde_json::from_str(msg_txt);
               match parse_res {
                   Ok(book_update @ BinanceBookUpdate{..}) => Some(book_update.into()),
                   _ => {
                       error!("Parse failed {:?}", msg_txt);
                       None
                   }
               }
           },
           _ => {
               error!("Receive failed {:?}", msg);
               None
           }
       }
    }
}

impl Stream for BinanceProvider {
    type Item = BookUpdate;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.ws_stream {
            Some(ws) => ws.as_mut().poll_next(cx).map(BinanceProvider::process_message),
            None => panic!("WebSocket not connected"),
        }
    }
}