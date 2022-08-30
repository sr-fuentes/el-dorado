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

This will subscribe to the SOL-PERP (default example) websocket stream and begin syncing the trades from 90 days prior to the first trade of the stream. Once the trades are synced they will be aggregated into 15 minute buckets and inserted into the database. At each new interval of 15 minutes the new candle will be aggregated and saved based on the trades captured from the websocket stream.

- Backfill and Validation of the captured trades can be run on separate instances with the `backfill` and `manage` commands respectively.

# System Design
![system_design](https://user-images.githubusercontent.com/29989568/183924160-51a64c5a-4e07-400f-9212-78795d8ba4d7.jpg)

# Workflows
## Run
![run](https://user-images.githubusercontent.com/29989568/183924327-e35a8aed-e023-475c-85b4-485920002a24.jpg)

## Backfill
![backfill](https://user-images.githubusercontent.com/29989568/183925850-c17bbb42-6dee-4954-b8a5-35476ca68d83.jpg)

## New Backfill Event
![new_fill_event](https://user-images.githubusercontent.com/29989568/183925938-937313c0-dead-41ec-9b2e-2d5c7e89684c.jpg)

# Acknowledgements and Forks
- [Zero2Prod](https://github.com/LukeMathWalker/zero-to-production)
  - After the Rust Book this was the most helpful resource in learning rust. This repository is roughly structured on the same concept of SQLX and Postgres as presented by Luke's course.
- [Rust FTX API](https://github.com/fabianboesiger/ftx)
  - Provided basic reqwest and websocket samples with mappings to FTX structs.
- [Rust Twilio API](https://github.com/neil-lobracco/twilio-rs)
  - Forked repository and removed outdated blocking requirements. Switched to [reqwest](https://github.com/seanmonstar/reqwest) from [hyper](https://github.com/hyperium/hyper) and removed functionality that was not used.
