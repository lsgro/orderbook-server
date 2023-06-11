# Tech challenge for Keyrock
_Luigi Sgro_ - [me@luigisgro.com](mailto:me@luigisgro.com)

## Requirements
* [Rust/Cargo](https://www.rust-lang.org/tools/install)
* [Protoc Protobuf compiler](https://github.com/protocolbuffers/protobuf/releases)

## Description
The demo application is composed of two modules:
* Server: listens on a port for gRPC connections, when a request is accepted
it connects to Binance and Bitstamp Websocket service, and starts
streaming snapshots of a consolidated book for each update received
* Client: connects to the gRPC server, printing to standard
output a dump of the snapshot received. It stops and disconnects
after a predefined number of snapshots is received.

## Run demo application
* Run the server: `cargo run --bin server`
* Run the client: `cargo run --bin client`
