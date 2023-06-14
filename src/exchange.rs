//! Common functionalities to create WebSocket exchange adapters and merging their
//! [streams](Stream) of data.

use log::{info, error};
use futures::prelude::*;
use std::{pin::Pin, task::{Context, Poll}};
use futures::stream::{Stream, select, Select};
use tokio::{time::{sleep, Duration}, sync::mpsc, net::TcpStream};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, tungstenite, MaybeTlsStream, WebSocketStream};


/// Delay before trying reconnection
const SLEEP_BEFORE_RECONNECT_MS: u64 = 200;


/// Type alias for an exchange-specific function that parses a message into an
/// [ExchangeProtocol](ExchangeProtocol) object.
/// 
/// # Generic arguments
/// 
/// * `T` - Output data type from the [exchange Stream](ExchangeAdapterStream).
pub type ExchangeProtocolReader<T> = &'static (dyn Fn(&str) -> Option<ExchangeProtocol<T>> + Send + Sync);

pub enum ExchangeProtocol<T: 'static + Send> {
    Data(T),
    ReconnectionRequest,
} 

/// Type used to send commands from the [exchange stream](ExchangeAdapterStream)
/// to the internal loop of the [exchange adapter](ExchangeAdapter).
enum AdapterCommand {
    /// Disconnect the exchange and exit the loop
    Close,
}

/// Contains all the information to connect to an exchange
pub struct ExchangeAdapter<T: 'static + Send> {
    /// Exchange code. Used for messages.
    exchange_code: &'static str,
    /// WebSocket URL.
    ws_url: String,
    /// WebSocket subscription message.
    subscribe_message: String,
    /// Exchange-specific message parser function.
    protocol_reader: ExchangeProtocolReader<T>,
}

impl <T: 'static + Send> ExchangeAdapter<T> {
    /// Create a new [ExchangeAdapter](ExchangeAdapter) object.
    ///
    /// # Arguments
    ///
    /// * `exchange_code` - The code of the exchange.
    ///
    /// * `ws_url` - WebSocket URL.
    ///
    /// * `subscribe_message` - WebSocket subscription message.
    ///
    /// * `protocol_reader` - Exchange-specific message parser function.
    ///
    /// # Returns
    ///
    /// A [ExchangeAdapter](ExchangeAdapter) object.
    pub async fn new(
        exchange_code: &'static str,
        ws_url: String,
        subscribe_message: String,
        protocol_reader: ExchangeProtocolReader<T>) -> ExchangeAdapter<T> {
        ExchangeAdapter {
            exchange_code,
            ws_url,
            subscribe_message,
            protocol_reader,
        }
    }

    /// Connects to the exchange WebSocket service and returns an object implementing [Stream](Stream).
    ///
    /// # Returns
    ///
    /// A [ExchangeAdapterStream](ExchangeAdapterStream) object.
    pub async fn make_stream(&self) -> ExchangeAdapterStream<T> {
        let exchange_code = self.exchange_code;
        let ws_url = self.ws_url.clone();
        let subscribe_message = self.subscribe_message.clone();
        let (data_sender, data_receiver) = mpsc::channel::<T>(16);
        let (command_sender, command_receiver) = mpsc::channel::<AdapterCommand>(1);
        tokio::spawn(
            Self::process_stream(
                exchange_code,
                ws_url,
                subscribe_message,
                self.protocol_reader,
                data_sender,
                command_receiver
            )
        );
        ExchangeAdapterStream {
            data_receiver,
            command_sender,
        }
    }

    /// Internal function implementing a loop reading from the exchange WebSocket service, and
    /// delivering the data received to the corresponding [ExchangeAdapterStream](ExchangeAdapterStream)
    /// object through a channel.
    /// It handles pings and it tries to reconnect in case of connection error.
    /// It receives [Command](AdapterCommand) instances through a channel, to drive its behavior.
    /// Currently only closing behavior implemented.
    async fn process_stream(
            exchange_code: &str,
            ws_url: String,
            subscribe_message: String,
            protocol_reader: ExchangeProtocolReader<T>,
            data_sender: mpsc::Sender<T>,
            mut command_receiver: mpsc::Receiver<AdapterCommand>) {
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
                        AdapterCommand::Close => {
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
                        if let Some(ExchangeProtocol::Data(data)) = protocol_reader(&text) {
                            match data_sender.send(data).await {
                                Ok(_) => (),
                                Err(_) => error!("Error queueing data"),
                            }
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

    /// Internal function performing a two step operation to create a functioning
    /// stream from an exchange WebSocket service:
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

/// Structure representing a connected exchange adapter.
pub struct ExchangeAdapterStream<T: 'static + Send> {
    /// Channel receiver for exchange data of type [T](T).
    data_receiver: mpsc::Receiver<T>,
    /// Channel sender for commands to drive the behaviour of the processing loop in the
    /// [ExchangeAdapter](ExchangeAdapter) object from which this structure is created.
    command_sender: mpsc::Sender<AdapterCommand>,
}

impl <T: 'static + Send> ExchangeAdapterStream<T> {
    /// Disconnect from the exchange.
    pub async fn disconnect(&mut self) {
        match self.command_sender.send(AdapterCommand::Close).await {
            Ok(_) => (),
            Err(_) => error!("Error queueing command"),
        };
    }
}

impl <T: 'static + Send> Stream for ExchangeAdapterStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.data_receiver.poll_recv(cx) {
            Poll::Ready(Some(data)) => {
                Poll::Ready(Some(data))
            }
            _ => Poll::Pending
        }
    }
}

/// Composite type containing multiple connections to exchanges. Since
/// [Select](Select) can only merge two streams at one time,
/// in order to use from 1 to n streams, a recursive structure is used.
pub enum ExchangeDataStream<T: 'static + Send> {
    /// Single exchange connection
    ExchangeStream(Pin<Box<ExchangeAdapterStream<T>>>),
    /// [Select](Select) of two [ExchangeDataStream](ExchangeDataStream) objects.
    CompositeStream(Pin<Box<Select<ExchangeDataStream<T>, ExchangeDataStream<T>>>>)
}

impl <T: 'static + Send> ExchangeDataStream<T> {
    /// Creates a new object from exchange adapters.
    ///
    /// # Arguments
    ///
    /// `exchange_adapters` - A reference to a [Vector](Vec) of [ExchangeAdapter](ExchangeAdapter) objects.
    ///
    /// # Returns
    ///
    /// An [ExchangeDataStream](ExchangeDataStream) object.
    pub async fn new(exchange_adapters: &Vec<ExchangeAdapter<T>>) -> ExchangeDataStream<T> {
        assert!(!exchange_adapters.is_empty());
        let mut adapter_streams: Vec<ExchangeAdapterStream<T>> = vec![];
        for p in exchange_adapters {
            let c = p.make_stream().await;
            adapter_streams.push(c);
        }
        if adapter_streams.len() > 1 {
            let mut wrapped_streams = adapter_streams.into_iter().map(
                |p| Self::ExchangeStream(Box::pin(p))
            );
            let w1 = wrapped_streams.next().unwrap();
            let w2 = wrapped_streams.next().unwrap();
            let acc = Self::CompositeStream(Box::pin(select(w1, w2)));
            wrapped_streams.fold(
                acc,
                |c, w| Self::CompositeStream(Box::pin(select(c, w))))
        } else {
            Self::ExchangeStream(Box::pin(adapter_streams.into_iter().next().unwrap()))
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

impl <T: 'static + Send> Stream for ExchangeDataStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Self::ExchangeStream(e) =>
                e.as_mut().poll_next(cx),
            Self::CompositeStream(c) =>
                c.as_mut().poll_next(cx)
        }
    }
}
