# El Dorado
The goal of this repository is to create:
1. A live price feed with key metrics for cryptocurrency exchanges based on a trailing 90 day data window for use with a trading engine.
2. A historical repository for trade prices and metrics for use with developing and backtesting trading strategies.

# Pre-requisites
- Rust
- Docker

# Setup and Install
- Launch a Postgres database in Docker with migrations
`./scripts/init_db.sh`
- Build with Cargo
`cargo build --release`

# Usage
- Run Live Feed
`./target/release/el-dorado run`

This will subscribe to the LDO-USD (default example) websocket stream and begin syncing the trades from 90 days prior to the first trade of the stream. Once the trades are synced they will be aggregated into 15 minute buckets and inserted into the database. At each new interval of 15 minutes the new candle will be aggregated and saved based on the trades captured from the websocket stream.

# Acknowledgements and Forks
- [Zero2Prod](https://github.com/LukeMathWalker/zero-to-production)
  - After the Rust Book this was the most helpful resource in learning rust. This repository is roughly structured on the same concept of SQLX and Postgres as presented by Luke's course.
- [Rust FTX API](https://github.com/fabianboesiger/ftx)
  - Provided basic reqwest and websocket samples with mappings to FTX structs.
- [Rust Twilio API](https://github.com/neil-lobracco/twilio-rs)
  - Forked repository and removed outdated blocking requirements. Switched to [reqwest](https://github.com/seanmonstar/reqwest) from [hyper](https://github.com/hyperium/hyper) and removed functionality that was not used.
