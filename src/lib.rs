//! Deterministic risk & liquidation engine for perpetual futures.
//!
//! See the README for the architecture. Module map:
//!
//! - [`fixed`]: exact fixed-point decimal arithmetic for money
//! - [`orderbook`]: sequence-checked L2 book from snapshot + delta feeds
//! - [`oracle`]: quorum/median mark price across venues
//! - [`margin`]: positions, market parameters, cross-margin accounting
//! - [`engine`]: the state machine — risk checks, liquidation, bad debt
//! - [`wal`]: write-ahead command log with group commit
//! - [`feed`]: venue WebSocket adapters with reconnect and read deadlines
//! - [`service`]: engine thread and oracle task wiring
//! - [`server`]: REST + WebSocket API
//! - [`sim`]: seeded market and trader simulation
//! - [`latency`]: allocation-free latency histogram

pub mod engine;
pub mod feed;
pub mod fixed;
pub mod latency;
pub mod margin;
pub mod oracle;
pub mod orderbook;
pub mod server;
pub mod service;
pub mod sim;
pub mod types;
pub mod wal;

pub use engine::{BACKSTOP, Command, Engine, Event, Reject};
pub use fixed::Fx;
pub use margin::{MarketConfig, Position};
pub use types::{AccountId, MarketId};

/// The default market set, shared by the binary, tests and benchmarks.
pub fn default_markets() -> Vec<MarketConfig> {
    let m = |id, symbol: &str, im, mm, fee| MarketConfig {
        id: MarketId(id),
        symbol: symbol.into(),
        initial_margin_bps: im,
        maintenance_margin_bps: mm,
        liquidation_fee_bps: fee,
        liquidation_chunk_bps: 2_500,
        price_band_bps: 100,
    };
    vec![
        m(1, "BTC-PERP", 500, 250, 100),
        m(2, "ETH-PERP", 667, 333, 125),
        m(3, "SOL-PERP", 1_000, 500, 200),
    ]
}
