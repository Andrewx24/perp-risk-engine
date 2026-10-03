//! Seeded market and trader simulation, so the whole pipeline can be run,
//! demoed and load-tested offline. Same seed, same run.
//!
//! The simulated venues are deliberately unreliable: books arrive as
//! snapshot + delta streams that occasionally drop a sequence number, one
//! venue periodically goes stale, another periodically prints prices far
//! from everyone else, and the underlying price occasionally gaps. Those are
//! exactly the conditions the book tracker, the oracle quorum and the
//! liquidation engine exist for.

use crate::engine::Command;
use crate::feed::{FeedMsg, VenueUpdate};
use crate::fixed::Fx;
use crate::orderbook::{Level, Side};
use crate::types::{AccountId, MarketId, now_ms};
use std::collections::BTreeMap;

/// SplitMix64: tiny, fast, and good enough for simulation.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    pub fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next_u64() % (hi - lo)
    }

    pub fn chance(&mut self, p: f64) -> bool {
        self.f64() < p
    }

    /// Standard normal via Box–Muller.
    pub fn normal(&mut self) -> f64 {
        let u1 = self.f64().max(f64::MIN_POSITIVE);
        let u2 = self.f64();
        (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    Normal,
    Stale { until_ms: u64 },
    Outlier { until_ms: u64, skew_bps: i64 },
}

struct SimVenue {
    name: &'static str,
    seq: u64,
    last: Option<(Vec<Level>, Vec<Level>)>,
    mode: Mode,
    noise_bps: f64,
}

pub struct SimMarket {
    pub market: MarketId,
    pub price: f64,
    pub tick: Fx,
    /// Per-step volatility (fraction of price).
    pub sigma: f64,
    /// Per-step probability of a jump.
    pub jump_prob: f64,
}

pub struct MarketSim {
    rng: Rng,
    markets: Vec<SimMarket>,
    venues: Vec<Vec<SimVenue>>,
    steps: u64,
}

const DEPTH: usize = 10;
pub const SIM_VENUES: [&str; 3] = ["sim-a", "sim-b", "sim-c"];

impl MarketSim {
    pub fn new(seed: u64, markets: Vec<SimMarket>) -> Self {
        let venues = markets
            .iter()
            .map(|_| {
                SIM_VENUES
                    .iter()
                    .enumerate()
                    .map(|(i, name)| SimVenue {
                        name,
                        seq: 1,
                        last: None,
                        mode: Mode::Normal,
                        noise_bps: 0.5 + i as f64,
                    })
                    .collect()
            })
            .collect();
        MarketSim {
            rng: Rng::new(seed),
            markets,
            venues,
            steps: 0,
        }
    }

    pub fn price(&self, market: MarketId) -> Option<f64> {
        self.markets
            .iter()
            .find(|m| m.market == market)
            .map(|m| m.price)
    }

    /// Advances every market one step and returns the venue messages.
    pub fn step(&mut self, out: &mut Vec<VenueUpdate>) {
        self.steps += 1;
        let ts = now_ms();
        for (mi, m) in self.markets.iter_mut().enumerate() {
            let rng = &mut self.rng;
            let mut ret = m.sigma * rng.normal();
            if rng.chance(m.jump_prob) {
                // Gaps are asymmetric in crypto: crashes are faster than rallies.
                ret += if rng.chance(0.7) {
                    -0.04 - 0.06 * rng.f64()
                } else {
                    0.03 * rng.f64()
                };
            }
            m.price *= 1.0 + ret;

            for v in self.venues[mi].iter_mut() {
                v.mode = match v.mode {
                    Mode::Stale { until_ms } | Mode::Outlier { until_ms, .. } if ts < until_ms => {
                        v.mode
                    }
                    _ if v.name == "sim-b" && rng.chance(0.0005) => Mode::Stale {
                        until_ms: ts + 4_000,
                    },
                    _ if v.name == "sim-c" && rng.chance(0.0005) => Mode::Outlier {
                        until_ms: ts + 2_000,
                        skew_bps: if rng.chance(0.5) { 400 } else { -400 },
                    },
                    _ => Mode::Normal,
                };
                if matches!(v.mode, Mode::Stale { .. }) {
                    continue;
                }
                let skew = match v.mode {
                    Mode::Outlier { skew_bps, .. } => skew_bps as f64 / 10_000.0,
                    _ => 0.0,
                };
                let mid = m.price * (1.0 + skew + v.noise_bps / 10_000.0 * rng.normal());
                let tick = m.tick.to_f64();
                let bids: Vec<Level> = (0..DEPTH)
                    .map(|i| {
                        let p = Fx::from_f64_lossy(mid - tick * (i as f64 + 0.5)).round_to(m.tick);
                        Level::new(p, Fx::from_f64_lossy(0.5 + 2.0 * rng.f64()))
                    })
                    .collect();
                let asks: Vec<Level> = (0..DEPTH)
                    .map(|i| {
                        let p = Fx::from_f64_lossy(mid + tick * (i as f64 + 0.5)).round_to(m.tick)
                            + m.tick;
                        Level::new(p, Fx::from_f64_lossy(0.5 + 2.0 * rng.f64()))
                    })
                    .collect();

                let msg = match &v.last {
                    // Re-snapshot periodically, like Bybit does, so a book
                    // that hit a gap recovers without a reconnect.
                    Some((pb, pa)) if self.steps % 200 != 0 => {
                        let mut changes = Vec::with_capacity(4 * DEPTH);
                        diff(Side::Bid, pb, &bids, &mut changes);
                        diff(Side::Ask, pa, &asks, &mut changes);
                        // Simulated packet loss: skip a sequence number.
                        if rng.chance(0.001) {
                            v.seq += 1;
                        }
                        FeedMsg::Delta {
                            first_seq: v.seq,
                            last_seq: v.seq,
                            changes,
                        }
                    }
                    _ => FeedMsg::Snapshot {
                        seq: v.seq,
                        bids: bids.clone(),
                        asks: asks.clone(),
                    },
                };
                v.seq += 1;
                v.last = Some((bids, asks));
                out.push(VenueUpdate {
                    venue: v.name,
                    market: m.market,
                    ts_ms: ts,
                    msg,
                });
            }
        }
    }
}

fn diff(side: Side, old: &[Level], new: &[Level], out: &mut Vec<(Side, Level)>) {
    for o in old {
        if !new.iter().any(|n| n.price == o.price) {
            out.push((side, Level::new(o.price, Fx::ZERO)));
        }
    }
    out.extend(new.iter().map(|n| (side, *n)));
}

/// Random traders: deposit, open leveraged positions, occasionally reduce.
pub struct TraderSim {
    rng: Rng,
    accounts: u64,
    funded: BTreeMap<AccountId, ()>,
}

impl TraderSim {
    pub fn new(seed: u64, accounts: u64) -> Self {
        TraderSim {
            rng: Rng::new(seed ^ 0x5eed),
            accounts,
            funded: BTreeMap::new(),
        }
    }

    pub fn next_command(&mut self, marks: &BTreeMap<MarketId, Fx>) -> Option<Command> {
        let account = AccountId(self.rng.range(1, self.accounts + 1));
        if !self.funded.contains_key(&account) || self.rng.chance(0.02) {
            self.funded.insert(account, ());
            let amount = Fx::from_int(self.rng.range(500, 50_000) as i64);
            return Some(Command::Deposit { account, amount });
        }
        let i = self.rng.range(0, marks.len().max(1) as u64) as usize;
        let (&market, &mark) = marks.iter().nth(i)?;
        // Notional between $100 and $200k; most get rejected for margin when
        // too large, which is the pre-trade check doing its job.
        let notional = 100.0 * (2000.0f64).powf(self.rng.f64());
        let sign = if self.rng.chance(0.5) { 1.0 } else { -1.0 };
        let qty = Fx::from_f64_lossy(sign * notional / mark.to_f64()).round_to(Fx::from_raw(1_000));
        let slip = 1.0 + 0.0005 * self.rng.normal();
        let price = Fx::from_f64_lossy(mark.to_f64() * slip);
        (!qty.is_zero()).then_some(Command::Trade {
            account,
            market,
            qty,
            price,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::BookTracker;

    #[test]
    fn simulated_books_track_the_price_and_recover_from_gaps() {
        let mut sim = MarketSim::new(
            42,
            vec![SimMarket {
                market: MarketId(1),
                price: 65_000.0,
                tick: Fx::from_raw(500_000),
                sigma: 0.0002,
                jump_prob: 0.0,
            }],
        );
        let mut t = BookTracker::default();
        let mut out = Vec::new();
        let mut mids = 0;
        for _ in 0..5_000 {
            out.clear();
            sim.step(&mut out);
            for u in &out {
                if let Some(mid) = t.on_update(u) {
                    mids += 1;
                    let p = sim.price(MarketId(1)).unwrap();
                    // Outlier venue can be 4% away; anything else is noise.
                    assert!((mid.to_f64() - p).abs() / p < 0.05, "{mid} vs {p}");
                }
            }
        }
        assert!(mids > 10_000);
    }

    #[test]
    fn rng_is_reproducible() {
        let (mut a, mut b) = (Rng::new(7), Rng::new(7));
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }
}
