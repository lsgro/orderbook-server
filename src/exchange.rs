//! Common functionalities to create WebSocket exchange adapters and merging their
//! [streams](Stream) of trading book snapshots.

use log::{info, error};
use futures::prelude::*;
use std::{pin::Pin, task::{Context, Poll}};
use futures::stream::{Stream, select, Select};
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream, tungstenite::protocol::Message, tungstenite};

use crate::core::*;


/// Parsing an exchange book snapshot string message into a [BookUpdate](BookUpdate) object.
/// Used by implementors of [BookUpdateSource](BookUpdateSource).
pub trait BookUpdateReader: Send + Sync {
    /// Read string and try to parse into a [BookUpdate](BookUpdate) object.
    ///
    /// # Arguments
    ///
    /// * `value` - A string
    ///
    /// # Returns
    ///
    /// If successful, a `Some(BookUpdate)`, `None` otherwise.
    fn read_book_update(&self, value: String) -> Option<BookUpdate>;
}

/// The required behavior of an exchange adapter. `Send` and `Sync` trait bounds
/// are needed to handle exchange adapter objects in a multi-threaded environment.
pub trait BookUpdateSource: Send + Sync {
    /// WebSocket base URL for the exchange service delivering continuous trading book snapshots.
    ///
    /// # Returns
    ///
    /// A string, to be used to build a complete WebSocket URL.
    fn ws_url(&self) -> String;

    /// Message to be sent to subscribe to continuous trading book snapshots.
    ///
    /// # Returns
    ///
    /// A string, to be sent to the exchange WebSocket service.
    fn subscribe_message(&self) -> String;

    /// Create a [BookUpdateReader](BookUpdateReader), wrapped in a [Box](Box).
    ///
    /// # Returns
    ///
    /// `Box` containing a [BookUpdateReader](BookUpdateReader) object.
    fn make_book_update_reader(&self) -> Box<dyn BookUpdateReader>;

    /// A string code identifying an exchange.
    ///
    /// # Return a static string slice.
    fn exchange_code(&self) -> &'static str;

    /// Create a new connection to the exchange. This is an asynchronous method, written without the
    /// `async` keyword, to be included in the trait.
    ///
    /// # Returns
    ///
    /// A pinned, boxed [Future](Future) object delivering an object of type
    /// [ConnectedBookUpdateSource](ConnectedBookUpdateSource) wrapped in a [Result](Result).
    fn make_connection(&self) -> Pin<Box<dyn Future<Output = Result<ConnectedBookUpdateSource, tungstenite::Error>> + Send + '_>> {
        let ws_url = self.ws_url();
        info!("Connecting to '{}'.", &ws_url);
        Box::pin(async move {
            let (mut ws, _) = connect_async(ws_url.clone()).await?;
            let subscribe_msg = self.subscribe_message();
            info!("Subscription '{}'.", &subscribe_msg);
            ws.send(Message::Text(subscribe_msg.clone())).await?;
            info!("Subscription '{}' succeeded.", &subscribe_msg);
            let book_update_reader = self.make_book_update_reader();
            Ok(ConnectedBookUpdateSource{
                exchange_code: self.exchange_code(),
                ws_stream: Box::pin(ws),
                book_update_reader,
            })
        })
    }
}

/// Object representing a connected exchange adapter.
pub struct ConnectedBookUpdateSource {
    /// Exchange code. Used for messages.
    exchange_code: &'static str,
    /// WebSocket stream delivering book snapshot messages from the exchange.
    ws_stream: Pin<Box<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>>,
    /// Exchange-specific message parser.
    book_update_reader: Box<dyn BookUpdateReader>,
}

impl ConnectedBookUpdateSource {
    /// Disconnect from the exchange.
    ///
    /// # Returns
    ///
    /// An empty [Result](Result).
    pub async fn disconnect(mut self) -> Result<(), tungstenite::Error>{
        info!("Disconnect from {}.", self.exchange_code);
        self.ws_stream.close().await
    }
}

/// A wrapper of the raw [Stream](Stream) from the WebSocket exchange service,
/// adding the functionalities of:
///
/// * parsing the original message into a [BookUpdate](BookUpdate) object.
///
/// * responding to `Ping` messages
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
                info!("Ping received from {}.", self.exchange_code);
                match futures::executor::block_on(self.ws_stream.send(Message::Pong(data))) {
                    Ok(()) => info!("Ping response sent."),
                    Err(e) => error!("Ping response send error {:?}", e)
                }
                Poll::Pending
            }
            _ => Poll::Pending
        }
    }
}

/// Composite type containing multiple connections to exchanges. Since
/// [Select](Select) can only merge two streams at one time,
/// in order to use from 1 to n streams, a recursive structure is used.
pub enum BookUpdateStream {
    /// Single exchange connection
    ExchangeStream(Pin<Box<ConnectedBookUpdateSource>>),
    /// [Select](Select) of two [BookUpdateStream](BookUpdateStream) objects.
    CompositeStream(Pin<Box<Select<BookUpdateStream, BookUpdateStream>>>)
}

impl BookUpdateStream {
    /// Creates a new object from single exchange adapters.
    ///
    /// # Arguments
    ///
    /// `exchange_sources` - A [Vector](Vec) of boxed [BookUpdateSource](BookUpdateSource) objects.
    ///
    /// # Returns
    ///
    /// A [BookUpdateStream](BookUpdateStream) object.
    pub async fn new(exchange_sources: &Vec<Box<dyn BookUpdateSource>>) -> Self {
        assert!(!exchange_sources.is_empty());
        let mut connected_sources: Vec<ConnectedBookUpdateSource> = vec![];
        for p in exchange_sources {
            let source_name = p.exchange_code();
            let c = p.make_connection().await.unwrap_or_else(|_| panic!("Connection error for {}", source_name));
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

    /// Disconnects all exchange adapters. This is an asynchronous method, which is written
    /// without the `async` keyword to allow for recursively calling itself.
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
