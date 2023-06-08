use log::info;
use futures::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream, tungstenite::protocol::Message};

use crate::core::*;


pub trait BookUpdateReader {
    fn read_book_update(&self, value: String) -> Option<BookUpdate>;
}

pub struct BookUpdateProvider {
    ws_url: String,
    subscribe_msg: String,
    book_update_reader: Box<dyn BookUpdateReader + Send>,
}

impl BookUpdateProvider {
    pub fn new(
        ws_url: String,
        subscribe_msg: String,
        book_update_reader: Box<dyn BookUpdateReader + Send>
    ) -> Self {
        BookUpdateProvider{ ws_url, subscribe_msg, book_update_reader }
    }

    pub async fn connect(self) -> Result<ConnectedBookUpdateProvider, ProviderError> {
        info!("Connecting to '{}'.", &self.ws_url);
        match connect_async(&self.ws_url).await {
            Ok((mut ws, _)) => {
                info!("Subscription '{}'.", &self.subscribe_msg);
                match ws.send(Message::Text(self.subscribe_msg.clone())).await {
                    Ok(_) => {
                        info!("Subscription '{}' succeeded.", &self.subscribe_msg);
                        Ok(ConnectedBookUpdateProvider{
                            ws_url: self.ws_url,
                            ws_stream: Box::pin(ws),
                            book_update_reader: self.book_update_reader,
                        })
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

pub struct ConnectedBookUpdateProvider {
    ws_url: String,
    ws_stream: Pin<Box<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>,
    book_update_reader: Box<dyn BookUpdateReader + Send>,
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