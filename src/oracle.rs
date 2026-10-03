//! Quorum mark-price oracle.
//!
//! Each venue is a voter. A mark price is only produced when at least
//! `min_sources` venues have a *fresh* observation **and** at least
//! `min_sources` of them agree with each other — within `max_deviation_bps`
//! of their median. The published price is the median of the agreeing set.
//!
//! With `2f + 1` sources the median is bounded by honest values even if `f`
//! of them are arbitrarily wrong (a frozen feed, a fat-finger print, a venue
//! being manipulated to trigger liquidations elsewhere). Refusing to publish
//! when there is no quorum is the point: the engine keeps the last good mark
//! and liquidations pause, rather than firing on one venue's bad tick.

use crate::fixed::Fx;
use serde::Serialize;
use std::collections::BTreeMap;

pub type SourceId = &'static str;

#[derive(Clone, Copy, Debug)]
pub struct OracleConfig {
    pub min_sources: usize,
    pub max_staleness_ms: u64,
    pub max_deviation_bps: i64,
}

impl Default for OracleConfig {
    fn default() -> Self {
        OracleConfig {
            min_sources: 2,
            max_staleness_ms: 2_000,
            max_deviation_bps: 100,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Observation {
    pub source: SourceId,
    pub price: Fx,
    pub ts_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Consensus {
    pub price: Fx,
    pub ts_ms: u64,
    pub agreeing: Vec<SourceId>,
    pub outliers: Vec<SourceId>,
    pub stale: Vec<SourceId>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize)]
pub enum OracleError {
    #[error("no quorum: {fresh} fresh sources, need {required}")]
    NoQuorum { fresh: usize, required: usize },
    #[error("sources disagree: {agreeing} within band, need {required}")]
    Disagreement { agreeing: usize, required: usize },
}

#[derive(Debug, Clone)]
pub struct PriceOracle {
    cfg: OracleConfig,
    latest: BTreeMap<SourceId, Observation>,
}

fn median(sorted: &[Fx]) -> Fx {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        Fx::from_raw((sorted[n / 2 - 1].raw() + sorted[n / 2].raw()) / 2)
    }
}

impl PriceOracle {
    pub fn new(cfg: OracleConfig) -> Self {
        PriceOracle {
            cfg,
            latest: BTreeMap::new(),
        }
    }

    /// Records an observation. Out-of-order observations from a source are
    /// dropped: a reconnecting feed must not roll the price backwards.
    pub fn submit(&mut self, obs: Observation) {
        match self.latest.get(obs.source) {
            Some(prev) if prev.ts_ms > obs.ts_ms => {}
            _ => {
                self.latest.insert(obs.source, obs);
            }
        }
    }

    pub fn aggregate(&self, now_ms: u64) -> Result<Consensus, OracleError> {
        let required = self.cfg.min_sources;
        let mut fresh: Vec<&Observation> = Vec::with_capacity(self.latest.len());
        let mut stale = Vec::new();
        for o in self.latest.values() {
            if now_ms.saturating_sub(o.ts_ms) <= self.cfg.max_staleness_ms && o.price.is_positive()
            {
                fresh.push(o);
            } else {
                stale.push(o.source);
            }
        }
        if fresh.len() < required {
            return Err(OracleError::NoQuorum {
                fresh: fresh.len(),
                required,
            });
        }

        fresh.sort_by_key(|o| o.price);
        let prices: Vec<Fx> = fresh.iter().map(|o| o.price).collect();
        let m = median(&prices);

        let (mut agreeing, mut outliers) = (Vec::new(), Vec::new());
        let mut kept = Vec::with_capacity(fresh.len());
        for o in &fresh {
            let dev = o.price.bps_from(m).unwrap_or(i64::MAX).abs();
            if dev <= self.cfg.max_deviation_bps {
                agreeing.push(o.source);
                kept.push(o.price);
            } else {
                outliers.push(o.source);
            }
        }
        if kept.len() < required {
            return Err(OracleError::Disagreement {
                agreeing: kept.len(),
                required,
            });
        }
        let ts_ms = fresh.iter().map(|o| o.ts_ms).max().unwrap_or(now_ms);
        Ok(Consensus {
            price: median(&kept),
            ts_ms,
            agreeing,
            outliers,
            stale,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(source: SourceId, price: i64, ts_ms: u64) -> Observation {
        Observation {
            source,
            price: Fx::from_int(price),
            ts_ms,
        }
    }

    fn oracle() -> PriceOracle {
        PriceOracle::new(OracleConfig {
            min_sources: 2,
            max_staleness_ms: 1_000,
            max_deviation_bps: 150,
        })
    }

    #[test]
    fn median_of_agreeing_sources() {
        let mut o = oracle();
        o.submit(obs("a", 100, 1_000));
        o.submit(obs("b", 101, 1_000));
        o.submit(obs("c", 100, 1_000));
        let c = o.aggregate(1_500).unwrap();
        assert_eq!(c.price, Fx::from_int(100));
        assert_eq!(c.agreeing.len(), 3);
    }

    #[test]
    fn one_lying_source_cannot_move_the_mark() {
        let mut o = oracle();
        o.submit(obs("a", 100, 1_000));
        o.submit(obs("b", 100, 1_000));
        o.submit(obs("evil", 90, 1_000));
        let c = o.aggregate(1_000).unwrap();
        assert_eq!(c.price, Fx::from_int(100));
        assert_eq!(c.outliers, vec!["evil"]);
    }

    #[test]
    fn stale_sources_do_not_vote() {
        let mut o = oracle();
        o.submit(obs("a", 100, 0));
        o.submit(obs("b", 100, 5_000));
        assert_eq!(
            o.aggregate(5_000),
            Err(OracleError::NoQuorum {
                fresh: 1,
                required: 2
            })
        );
    }

    #[test]
    fn split_brain_publishes_nothing() {
        let mut o = oracle();
        o.submit(obs("a", 100, 0));
        o.submit(obs("b", 120, 0));
        assert!(matches!(
            o.aggregate(0),
            Err(OracleError::Disagreement { .. })
        ));
    }

    #[test]
    fn out_of_order_observation_is_dropped() {
        let mut o = oracle();
        o.submit(obs("a", 100, 2_000));
        o.submit(obs("a", 50, 1_000));
        o.submit(obs("b", 100, 2_000));
        assert_eq!(o.aggregate(2_000).unwrap().price, Fx::from_int(100));
    }
}
