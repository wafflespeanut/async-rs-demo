## async-rs-demo

[![Build Status](https://api.travis-ci.org/wafflespeanut/async-rs-demo.svg?branch=master)](https://travis-ci.org/wafflespeanut/async-rs-demo)

A hobby project for designing a high performance order book aggregation and streaming service in Rust leveraging the async/await ecosystem.

Briefly, this has a gRPC server and a bunch of background processors which connect to some configured exchanges ([Binance](https://www.binance.com/en) and [Bitstamp](https://www.bitstamp.net/) as of now) through websockets. A gRPC client can connect to the server and subscribe to order book summary for some symbol (say, "ETHBTC"). Once subscribed, the client will receive a stream of order book summary for that particular symbol from all configured exchanges with "asks" and "bids" merged and sorted accordingly.

The server itself is not restricted to one symbol. If a new client asks for order summary of a symbol, the server checks if it already has it, and if it doesn't, it'll subscribe, aggregate and send the order summary.

### Usage

- Make sure you have Rust installed with some channel (https://rustup.rs/).
- Run `cargo test` to run tests.
- Run `cargo build` to build the app (pass `--release` flag for production). This will create `aggregator-client` and `aggregator-server` in build directory.
- Once built, you can spawn the server by running `./target/debug/aggregator-server` (for release build, it's `./target/release/aggregator-server`).
- In another shell, `export SYMBOL=ethbtc` (or some symbol of your choice).
- Launch the default client by running `./target/debug/aggregator-client` (or `./target/release/aggregator-client`) - it should stream order book summary for that symbol.

**Environment variables:**
 - `SYMBOL`: Sets the subscription symbol for client.
 - `GRPC_ADDR`: Overrides the default gRPC address (`[::1]:9051`) for client/server.
 - `RUST_LOG`: Sets logging for client/server. For example, setting `RUST_LOG=aggregator_server=trace` to the server enables `TRACE` (lowest) level of logging for the server and no logging for other crates.

### Design

This completely leverages the async/await support in Rust which stabilized a few months back. There are no blocking calls throughout and it's quite fast. These are the components involved:
- **Server:** A gRPC server which serves connections from gRPC clients. It registers clients in the *merger* and streams order book summary to these clients from the *merger*.
- **Processor:** This takes care of a single websocket connection (to some exchange).  This means, there's a processor for each exchange. It connects/reconnects to websocket servers, subscribes/resubscribes to order books for symbols upon request from *merger*, processes responses and sends generic order books to the *merger*.
- **Merger:** This has two tasks in the background.
  - *Aggregation:* This receives order books from *processors*, merges and sorts them to generate summary and sends it to *fanout*.
  - *Fanout:* When it receives a registration request for an incoming client from *gRPC server*, it notifies all *processors* of a subscription and adds it to the collection. When it receives a summary from *aggregation*, it sends the message to
  all clients in the collection. It also filters dropped connections in this process.

### Notes

1. Is it possible for the gRPC clients to send an invalid symbol? The websocket servers in exchanges don't throw any errors for invalid symbols. So, as of now, if too many clients send invalid symbols, then the processor will keep accruing those subscriptions. We could get around this by validating incoming symbols against known symbols or maintaining a timeout for order books received from exchanges and if there's none over that set period of time, we can remove the symbol from subscription.
2. This assumes that exchanges support subscribing to order books for multiple symbolx in the same websocket (Binance and Bitstamp do that), but if we encounter an exchange which doesn't do this, then the *processor* needs to be slightly rethought (same processor with multiple websockets per symbol? processor for a symbol/exchange pair?).
3. There are unit tests for sorting and merging order books, but for extensive testing, we should go for end-to-end tests where we actually spin up a websocket server, implement `Processor` trait and test possible cases.
4. Can we trust the exchange? As of now, we filter `NaN` and invalid values, we sort order books by ourselves, but what if we got a negative spread value? i.e., the best bid is higher than the best ask?
