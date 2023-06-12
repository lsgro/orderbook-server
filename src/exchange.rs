use log::info;
use futures::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream, tungstenite::protocol::Message};

use crate::core::*;


pub trait BookUpdateReader: Send + Sync {
    fn read_book_update(&self, value: String) -> Option<BookUpdate>;
}

pub trait BookUpdateProvider: Send + Sync {
    fn ws_url(&self) -> String;

    fn subscribe_message(&self) -> String;

    fn make_book_update_reader(&self) -> Box<dyn BookUpdateReader>;

    fn name(&self) -> &'static str;

    fn make_connection(&self) -> Pin<Box<dyn Future<Output = Result<ConnectedBookUpdateProvider, ProviderError>> + Send + '_>> {
        let ws_url = self.ws_url();
        info!("Connecting to '{}'.", &ws_url);
        Box::pin(async move {
            match connect_async(ws_url.clone()).await {
                Ok((mut ws, _)) => {
                    let subscribe_msg = self.subscribe_message();
                    info!("Subscription '{}'.", &subscribe_msg);
                    match ws.send(Message::Text(subscribe_msg.clone())).await {
                        Ok(_) => {
                            info!("Subscription '{}' succeeded.", &subscribe_msg);
                            let book_update_reader = self.make_book_update_reader();
                            Ok(ConnectedBookUpdateProvider{
                                ws_url: ws_url.clone(),
                                ws_stream: Box::pin(ws),
                                book_update_reader: book_update_reader,
                            })
                        },
                        _ => Err(ProviderError::Io)
                    }
                },
                _ => {
                    Err(ProviderError::Io)
                }
            }
        })
    }
}

pub struct ConnectedBookUpdateProvider {
    ws_url: String,
    ws_stream: Pin<Box<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>,
    book_update_reader: Box<dyn BookUpdateReader>,
}

impl ConnectedBookUpdateProvider {
    pub async fn disconnect(mut self) -> Result<(), ProviderError>{
        info!("Disconnect from '{}'.", &self.ws_url);
        self.ws_stream.close().await.map_err(|_| ProviderError::Io)
    }
}

impl Stream for ConnectedBookUpdateProvider {
    type Item = BookUpdate;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.ws_stream.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(Message::Text(msg_txt)))) => {
                match self.book_update_reader.read_book_update(msg_txt) {
                    maybe_book_update @ Some(_) => Poll::Ready(maybe_book_update),
                    _ => Poll::Pending
                }
            }
            Poll::Ready(Some(Ok(Message::Ping(data)))) => {
                info!("Ping received from '{}'. Respond.", &self.ws_url);
                let _ = self.ws_stream.send(Message::Pong(data));
                Poll::Pending
            }
            _ => Poll::Pending
        }
    }
}