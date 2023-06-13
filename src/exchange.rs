//! Common functionalities to create WebSocket exchange adapters and merging their
//! [streams](Stream) of trading book snapshots.

use log::{info, error};
use futures::prelude::*;
use std::{pin::Pin, task::{Context, Poll}};
use futures::stream::{Stream, select, Select};
use tokio::{time::{sleep, Duration}, sync::mpsc, net::TcpStream};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, tungstenite, MaybeTlsStream, WebSocketStream};

use crate::core::*;


/// Delay before trying reconnection
const SLEEP_BEFORE_RECONNECT_MS: u64 = 200;


/// Type alias for an exchange-specific function that parses a message into a
/// [BookUpdate](BookUpdate) object
pub type BookUpdateReader = &'static (dyn Fn(&str) -> Option<BookUpdate> + Send + Sync);

/// Type used to send commands from the [exchange stream source](BookUpdateSourceStream)
/// to the internal loop of the [exchange source](BookUpdateSource).
enum Command {
    /// Disconnect the exchange and exit the loop
    Close,
}

/// Contains all the information to connect to an exchange
pub struct BookUpdateSource {
    /// Exchange code. Used for messages.
    exchange_code: &'static str,
    /// WebSocket URL.
    ws_url: String,
    /// WebSocket subscription message.
    subscribe_message: String,
    /// Exchange-specific message parser function.
    book_update_reader: BookUpdateReader,
}

impl BookUpdateSource {
    /// Create a new [BookUpdateSource](BookUpdateSource) object.
    ///
    /// # Arguments
    ///
    /// * `exchange_code` - The code of the exchange.
    ///
    /// * `ws_url` - WebSocket URL.
    ///
    /// * `subscribe_message` - WebSocket subscription message.
    ///
    /// * `book_update_reader` - Exchange-specific message parser function.
    ///
    /// # Returns
    ///
    /// A [BookUpdateSource](BookUpdateSource) object.
    pub async fn new(
            exchange_code: &'static str,
            ws_url: String,
            subscribe_message: String,
            book_update_reader: BookUpdateReader) -> BookUpdateSource {
        BookUpdateSource {
            exchange_code,
            ws_url,
            subscribe_message,
            book_update_reader,
        }
    }

    /// Connects to the exchange WebSocket service and returns an object implementing [Stream](Stream).
    ///
    /// # Returns
    ///
    /// A [BookUpdateSourceStream](BookUpdateSourceStream) object.
    pub async fn make_stream(&self) -> BookUpdateSourceStream {
        let exchange_code = self.exchange_code;
        let ws_url = self.ws_url.clone();
        let subscribe_message = self.subscribe_message.clone();
        let (data_sender, data_receiver) = mpsc::channel::<String>(1);
        let (command_sender, command_receiver) = mpsc::channel::<Command>(1);
        tokio::spawn(
            Self::process_stream(exchange_code, ws_url, subscribe_message, data_sender, command_receiver)
        );
        BookUpdateSourceStream {
            data_receiver,
            command_sender,
            book_update_reader: self.book_update_reader,
        }
    }

    /// Internal function implementing a loop reading from the exchange WebSocket service, and
    /// delivering the data received to the corresponding [BookUpdateSourceStream](BookUpdateSourceStream)
    /// object through a channel.
    /// It handles pings and it tries to reconnect in case of connection error.
    /// It receives [Command](Command) instances through a channel, to drive its behavior.
    /// Currently only closing behavior implemented.
    async fn process_stream(
            exchange_code: &str,
            ws_url: String,
            subscribe_message: String,
            data_sender: mpsc::Sender<String>,
            mut command_receiver: mpsc::Receiver<Command>) {
        'connection:
        loop {
            let mut pinned_ws = Self::connect(
                exchange_code,
                ws_url.clone(),
                subscribe_message.clone()
            ).await;
            loop {
                if let Ok(command) = command_receiver.try_recv() {
                    match command {
                        Command::Close => {
                            info!("Disconnecting exchange {}", exchange_code);
                            match pinned_ws.close().await {
                                Ok(_) => info!("Exchange {} disconnected", exchange_code),
                                Err(error) => error!("Error disconnecting from {}: {:?}", exchange_code, error),
                            }
                            break 'connection;
                        }
                    }
                }
                match pinned_ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        match data_sender.send(text).await {
                            Ok(_) => (),
                            Err(_) => error!("Error queueing data"),
                        }
                    },
                    Some(Ok(Message::Ping(data))) => {
                        info!("Received ping from {}", exchange_code);
                        match pinned_ws.send(Message::Pong(data)).await {
                            Ok(_) => info!("Sent ping response to {}", exchange_code),
                            Err(_) => error!("Error sending ping response to {}", exchange_code),
                        }
                    },
                    Some(Err(
                             tungstenite::Error::AlreadyClosed |
                             tungstenite::Error::Io(_)
                         )
                    ) => {
                        error!("Connection to exchange {} closed", exchange_code);
                        break
                    },
                    Some(other) => info!("Received unexpected message: {:?}", other),
                    _ => (),
                }
            }
            info!("Trying reconnection in {}ms", SLEEP_BEFORE_RECONNECT_MS);
            sleep(Duration::from_millis(SLEEP_BEFORE_RECONNECT_MS)).await;
        }
    }

    /// Internal function performing a two step operation to create a functioning WebSocket
    /// source:
    /// * Connecting to the WebSocket URL
    /// * Sending a message to subscribe to the relevant channel
    /// It panics in case of error.
    async fn connect(
            exchange_code: &str,
            ws_url: String,
            subscribe_message: String) -> Pin<Box<WebSocketStream<MaybeTlsStream<TcpStream>>>> {
        info!("Connecting to WebSocket: {}", &ws_url);
        let (ws, _) = connect_async(ws_url.clone()).await.unwrap_or_else(
            |_| panic!("Connection error for {}", exchange_code));
        info!("Subscription '{}'.", subscribe_message);
        let mut pinned_ws = Box::pin(ws);
        pinned_ws.send(Message::Text(subscribe_message.clone())).await.unwrap_or_else(
            |_| panic!("Subscription error for {}", subscribe_message));
        info!("Subscription to {} succeeded.", exchange_code);
        pinned_ws
    }
}

/// Structure representing a connected exchange source.
pub struct BookUpdateSourceStream {
    /// Channel receiver receiving string messages from the WebSocket.
    data_receiver: mpsc::Receiver<String>,
    /// Channel sender for commands to drive the behaviour of the processing loop in the
    /// [BookUpdateSource](BookUpdateSource) object from which this structure is created.
    command_sender: mpsc::Sender<Command>,
    /// Exchange-specific message parser function.
    book_update_reader: BookUpdateReader,
}

impl BookUpdateSourceStream {
    /// Disconnect from the exchange.
    pub async fn disconnect(&mut self) {
        match self.command_sender.send(Command::Close).await {
            Ok(_) => (),
            Err(_) => error!("Error queueing command"),
        };
    }
}

impl Stream for BookUpdateSourceStream {
    type Item = BookUpdate;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.data_receiver.poll_recv(cx) {
            Poll::Ready(Some(text)) => {
                if let Some(book_update) = (self.book_update_reader)(&text) {
                    Poll::Ready(Some(book_update))
                } else {
                    info!("Could not parse message {}", &text);
                    Poll::Pending
                }
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
    ExchangeStream(Pin<Box<BookUpdateSourceStream>>),
    /// [Select](Select) of two [BookUpdateStream](BookUpdateStream) objects.
    CompositeStream(Pin<Box<Select<BookUpdateStream, BookUpdateStream>>>)
}

impl  BookUpdateStream {
    /// Creates a new object from exchange adapters.
    ///
    /// # Arguments
    ///
    /// `exchange_sources` - A reference to a [Vector](Vec) of [BookUpdateSource](BookUpdateSource) objects.
    ///
    /// # Returns
    ///
    /// A [BookUpdateStream](BookUpdateStream) object.
    pub async fn new(exchange_sources: &Vec<BookUpdateSource>) -> BookUpdateStream {
        assert!(!exchange_sources.is_empty());
        let mut connected_sources: Vec<BookUpdateSourceStream> = vec![];
        for p in exchange_sources {
            let c = p.make_stream().await;
            connected_sources.push(c);
        }
        if connected_sources.len() > 1 {
            let mut wrapped_sources = connected_sources.into_iter().map(
                |p| Self::ExchangeStream(Box::pin(p))
            );
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

    /// Disconnects all exchange adapters. Asynchronous recursive method.
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
