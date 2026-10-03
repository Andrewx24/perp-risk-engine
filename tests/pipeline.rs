//! End-to-end, single-threaded: simulated venues → book tracker → quorum
//! oracle → engine, with simulated traders, for a few simulated minutes.
//! No Tokio and no wall clock in the loop, so the run is reproducible.

use perp_risk_engine::engine::Event;
use perp_risk_engine::feed::BookTracker;
use perp_risk_engine::oracle::{Observation, OracleConfig, PriceOracle};
use perp_risk_engine::sim::{MarketSim, SimMarket, TraderSim};
use perp_risk_engine::{Command, Engine, Fx, MarketId, default_markets};
use std::collections::BTreeMap;

fn run(seed: u64, steps: usize) -> (Engine, usize, usize) {
    let markets = [MarketId(1), MarketId(2), MarketId(3)];
    let mut sim = MarketSim::new(
        seed,
        vec![
            SimMarket {
                market: markets[0],
                price: 65_000.0,
                tick: Fx::from_raw(500_000),
                sigma: 0.0005,
                jump_prob: 0.002,
            },
            SimMarket {
                market: markets[1],
                price: 3_200.0,
                tick: Fx::from_raw(10_000),
                sigma: 0.0006,
                jump_prob: 0.002,
            },
            SimMarket {
                market: markets[2],
                price: 150.0,
                tick: Fx::from_raw(1_000),
                sigma: 0.0008,
                jump_prob: 0.003,
            },
        ],
    );
    let mut traders = TraderSim::new(seed, 300);
    let mut tracker = BookTracker::default();
    let cfg = OracleConfig {
        min_sources: 2,
        max_staleness_ms: u64::MAX,
        max_deviation_bps: 100,
    };
    let mut oracles: BTreeMap<MarketId, PriceOracle> = BTreeMap::new();

    let mut engine = Engine::new();
    let mut out = Vec::new();
    for m in default_markets() {
        engine.apply(&Command::CreateMarket(m), &mut out);
    }

    let mut updates = Vec::new();
    let mut log = Vec::new();
    for step in 0..steps {
        updates.clear();
        sim.step(&mut updates);
        for u in &updates {
            if let Some(mid) = tracker.on_update(u) {
                // Logical time: the step number, not the wall clock.
                let o = oracles
                    .entry(u.market)
                    .or_insert_with(|| PriceOracle::new(cfg));
                o.submit(Observation {
                    source: u.venue,
                    price: mid,
                    ts_ms: step as u64,
                });
            }
        }
        for m in markets {
            if let Some(c) = oracles.get(&m).and_then(|o| o.aggregate(step as u64).ok()) {
                log.push(Command::Mark {
                    market: m,
                    price: c.price,
                });
            }
        }
        for c in log.drain(..) {
            engine.apply(&c, &mut out);
        }
        for _ in 0..20 {
            if let Some(c) = traders.next_command(engine.marks()) {
                engine.apply(&c, &mut out);
            }
        }
        engine.check_invariants().unwrap();
    }
    let liqs = out
        .iter()
        .filter(|e| matches!(e, Event::Liquidated { .. }))
        .count();
    let trades = out
        .iter()
        .filter(|e| matches!(e, Event::TradeExecuted { .. }))
        .count();
    (engine, liqs, trades)
}

#[test]
fn simulated_session_trades_liquidates_and_stays_consistent() {
    let (engine, liqs, trades) = run(7, 3_000);
    assert!(trades > 1_000, "trades: {trades}");
    assert!(liqs > 0, "a session with jumps should liquidate someone");
    engine.check_no_liquidatable().unwrap();
    assert!(engine.insurance_fund() >= Fx::ZERO);
}

#[test]
fn same_seed_same_transcript() {
    let (a, _, _) = run(11, 500);
    let (b, _, _) = run(11, 500);
    assert_eq!(a.transcript_hash(), b.transcript_hash());
}
