# Book aggregator server
Protobuf gRPC streaming server providing continuous trading book snapshots
from multiple digital currency exchanges.
Currently implemented for [Binance](https://binance.com) and [Bitstamp](https://bitstamp.net).

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

_Author_: _Luigi Sgro_

_email_: [me@luigisgro.com](mailto:me@luigisgro.com)

## Requirements
* [Rust/Cargo version 1.7.0](https://www.rust-lang.org/tools/install)
* [Protoc Protobuf compiler - libprotoc version 23.2](https://github.com/protocolbuffers/protobuf/releases)

## Description
The demo application is composed of two modules:
* Server: listens on a port for gRPC connections, when a request is accepted
it connects to Binance and Bitstamp Websocket service, and starts
streaming snapshots of a consolidated book for each update received
* Client: connects to the gRPC server, printing to standard
output a dump of the snapshot received. It stops and disconnects
after a predefined number of snapshots is received.

## Compile, test and generate documentation
```shell
cargo build --bin server
cargo build --bin client
cargo test
cargo doc --no-deps --document-private-items
```
HTML documentation index is generated in `./target/doc/orderbook_server/index.html`.

## Run demo application
* Run the server: `cargo run --bin server ETH-BTC`.
Optionally specify a port as last argument.
* Run the client (on the same host): `cargo run --bin client`.
Specify a port as last argument if the server was started with a port argument.
