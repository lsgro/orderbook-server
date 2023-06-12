use log::info;
use futures::prelude::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures::stream::{Stream, select, Select};
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream, tungstenite::protocol::Message};

use crate::core::*;


pub trait BookUpdateReader: Send + Sync {
    fn read_book_update(&self, value: String) -> Option<BookUpdate>;
}

pub trait BookUpdateSource: Send + Sync {
    fn ws_url(&self) -> String;

    fn subscribe_message(&self) -> String;

    fn make_book_update_reader(&self) -> Box<dyn BookUpdateReader>;

    fn name(&self) -> &'static str;

    fn make_connection(&self) -> Pin<Box<dyn Future<Output = Result<ConnectedBookUpdateSource, SourceError>> + Send + '_>> {
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
                            Ok(ConnectedBookUpdateSource{
                                ws_url: ws_url.clone(),
                                ws_stream: Box::pin(ws),
                                book_update_reader: book_update_reader,
                            })
                        },
                        _ => Err(SourceError::Io)
                    }
                },
                _ => {
                    Err(SourceError::Io)
                }
            }
        })
    }
}

pub struct ConnectedBookUpdateSource {
    ws_url: String,
    ws_stream: Pin<Box<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>,
    book_update_reader: Box<dyn BookUpdateReader>,
}

impl ConnectedBookUpdateSource {
    pub async fn disconnect(mut self) -> Result<(), SourceError>{
        info!("Disconnect from '{}'.", &self.ws_url);
        self.ws_stream.close().await.map_err(|_| SourceError::Io)
    }
}

impl Stream for ConnectedBookUpdateSource {
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

/// Structure containing multiple exchange data Sources, merged together with
/// (future::stream::Select)[future::stream::Select].
/// Since this structure can only merge two streams at one time, in order to use from 1 to n
/// streams, a recursive structure is used.
/// Maintaining the source (ConnectedBookUpdateSource)[ConnectedBookUpdateSource] makes it
/// possible to disconnect from the exchanges.
pub enum BookUpdateStream {
    ExchangeStream(Pin<Box<ConnectedBookUpdateSource>>),
    CompositeStream(Pin<Box<Select<BookUpdateStream, BookUpdateStream>>>)
}

impl BookUpdateStream {
    pub async fn new(exchange_sources: &Vec<Box<dyn BookUpdateSource>>) -> Self {
        assert!(!exchange_sources.is_empty());
        let mut connected_sources: Vec<ConnectedBookUpdateSource> = vec![];
        for p in exchange_sources {
            let source_name = p.name();
            let c = p.make_connection().await.expect(&format!("Connection error for {}", source_name));
            connected_sources.push(c);
        }
        if connected_sources.len() > 1 {
            let mut wrapped_sources = connected_sources.into_iter().map(
                |p| Self::ExchangeStream(Box::pin(p)));
            let w1 = wrapped_sources.next().unwrap();
            let w2 = wrapped_sources.next().unwrap();
            let acc = Self::CompositeStream(Box::pin(select(w1, w2)));
            wrapped_sources.fold(
                acc,
                |c, w| Self::CompositeStream(Box::pin(select(c, w))))
        } else {
            Self::ExchangeStream(Box::pin(connected_sources.into_iter().next().unwrap()))
        }
    }

    pub fn disconnect(self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            match self {
                Self::ExchangeStream(p) => {
                    let _ = Pin::into_inner(p).disconnect().await;
                },
                Self::CompositeStream(s) => {
                    let (s1, s2) = Pin::into_inner(s).into_inner();
                    s1.disconnect().await;
                    s2.disconnect().await;
                }
            };
        })
    }
}

impl Stream for BookUpdateStream {
    type Item = BookUpdate;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::ExchangeStream(e) =>
                e.as_mut().poll_next(cx),
            Self::CompositeStream(c) =>
                c.as_mut().poll_next(cx)
        }
    }
}
