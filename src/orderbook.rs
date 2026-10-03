//! L2 order book reconstructed from a venue's snapshot + incremental feed.
//!
//! The hard part of a book is not storing levels, it is knowing when your copy
//! is wrong. Every update carries a sequence range; an update that does not
//! start where the last one ended means a message was lost, and every level
//! derived after that point is fiction. On a gap the book clears itself and
//! refuses to quote until a fresh snapshot arrives — a stale mid fed into the
//! oracle is far worse than a missing one.

use crate::fixed::Fx;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub price: Fx,
    pub qty: Fx,
}

impl Level {
    pub fn new(price: Fx, qty: Fx) -> Self {
        Level { price, qty }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BookError {
    #[error("sequence gap: expected {expected}, got {got}")]
    SequenceGap { expected: u64, got: u64 },
    #[error("book not synced; waiting for snapshot")]
    NotSynced,
    #[error("crossed book: bid {bid} >= ask {ask}")]
    Crossed { bid: Fx, ask: Fx },
}

#[derive(Debug, Default, Clone)]
pub struct OrderBook {
    bids: BTreeMap<Fx, Fx>,
    asks: BTreeMap<Fx, Fx>,
    last_seq: Option<u64>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_synced(&self) -> bool {
        self.last_seq.is_some()
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    pub fn apply_snapshot(
        &mut self,
        seq: u64,
        bids: &[Level],
        asks: &[Level],
    ) -> Result<(), BookError> {
        self.bids.clear();
        self.asks.clear();
        for l in bids.iter().filter(|l| l.qty.is_positive()) {
            self.bids.insert(l.price, l.qty);
        }
        for l in asks.iter().filter(|l| l.qty.is_positive()) {
            self.asks.insert(l.price, l.qty);
        }
        self.last_seq = Some(seq);
        self.check_crossed()
    }

    /// Applies an update covering sequence numbers `first_seq..=last_seq`.
    ///
    /// Returns `Ok(false)` for an update entirely older than the book (normal
    /// right after a snapshot, when buffered deltas are replayed). An update
    /// that skips ahead desyncs the book and returns the gap.
    pub fn apply_delta(
        &mut self,
        first_seq: u64,
        last_seq: u64,
        changes: &[(Side, Level)],
    ) -> Result<bool, BookError> {
        let cur = self.last_seq.ok_or(BookError::NotSynced)?;
        if last_seq <= cur {
            return Ok(false);
        }
        if first_seq > cur + 1 {
            self.desync();
            return Err(BookError::SequenceGap {
                expected: cur + 1,
                got: first_seq,
            });
        }
        for (side, l) in changes {
            let book = match side {
                Side::Bid => &mut self.bids,
                Side::Ask => &mut self.asks,
            };
            if l.qty.is_positive() {
                book.insert(l.price, l.qty);
            } else {
                book.remove(&l.price);
            }
        }
        self.last_seq = Some(last_seq);
        self.check_crossed().map(|_| true)
    }

    fn check_crossed(&mut self) -> Result<(), BookError> {
        if let (Some(b), Some(a)) = (self.best_bid(), self.best_ask())
            && b.price >= a.price
        {
            self.desync();
            return Err(BookError::Crossed {
                bid: b.price,
                ask: a.price,
            });
        }
        Ok(())
    }

    pub fn desync(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_seq = None;
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids
            .iter()
            .next_back()
            .map(|(p, q)| Level::new(*p, *q))
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.iter().next().map(|(p, q)| Level::new(*p, *q))
    }

    pub fn mid(&self) -> Option<Fx> {
        if !self.is_synced() {
            return None;
        }
        let (b, a) = (self.best_bid()?, self.best_ask()?);
        Some(Fx::from_raw((b.price.raw() + a.price.raw()) / 2))
    }

    pub fn spread_bps(&self) -> Option<i64> {
        let (b, a, mid) = (self.best_bid()?, self.best_ask()?, self.mid()?);
        a.price
            .bps_from(mid)
            .map(|half| half * 2)
            .filter(|_| b.price < a.price)
    }

    /// Volume-weighted price to execute `qty` against the book (positive
    /// buys from the asks, negative sells into the bids). `None` when the
    /// visible book cannot absorb the size — the caller must decide what a
    /// liquidation does when there is no liquidity, not this function.
    pub fn impact_price(&self, qty: Fx) -> Option<Fx> {
        if qty.is_zero() || !self.is_synced() {
            return None;
        }
        let mut remaining = qty.abs();
        let mut notional = Fx::ZERO;
        let mut walk = |price: Fx, avail: Fx| {
            let take = if avail < remaining { avail } else { remaining };
            notional += take.mul_trunc(price);
            remaining -= take;
            remaining.is_zero()
        };
        let filled = if qty.is_positive() {
            self.asks.iter().any(|(p, q)| walk(*p, *q))
        } else {
            self.bids.iter().rev().any(|(p, q)| walk(*p, *q))
        };
        if !filled {
            return None;
        }
        notional.checked_div(qty.abs())
    }

    /// Top `n` levels per side, best first.
    pub fn depth(&self, n: usize) -> (Vec<Level>, Vec<Level>) {
        let bids = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(p, q)| Level::new(*p, *q))
            .collect();
        let asks = self
            .asks
            .iter()
            .take(n)
            .map(|(p, q)| Level::new(*p, *q))
            .collect();
        (bids, asks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn l(p: i64, q: i64) -> Level {
        Level::new(Fx::from_int(p), Fx::from_int(q))
    }

    fn book() -> OrderBook {
        let mut b = OrderBook::new();
        b.apply_snapshot(
            100,
            &[l(99, 1), l(98, 2), l(97, 5)],
            &[l(101, 1), l(102, 2), l(103, 5)],
        )
        .unwrap();
        b
    }

    #[test]
    fn top_of_book_and_mid() {
        let b = book();
        assert_eq!(b.best_bid(), Some(l(99, 1)));
        assert_eq!(b.best_ask(), Some(l(101, 1)));
        assert_eq!(b.mid(), Some(Fx::from_int(100)));
    }

    #[test]
    fn stale_deltas_are_ignored_and_contiguous_ones_applied() {
        let mut b = book();
        assert_eq!(b.apply_delta(90, 100, &[(Side::Bid, l(99, 0))]), Ok(false));
        assert_eq!(b.best_bid(), Some(l(99, 1)));
        // Overlapping the snapshot boundary is allowed (Binance-style U <= id+1 <= u).
        assert_eq!(b.apply_delta(95, 101, &[(Side::Bid, l(99, 0))]), Ok(true));
        assert_eq!(b.best_bid(), Some(l(98, 2)));
    }

    #[test]
    fn gap_desyncs_until_snapshot() {
        let mut b = book();
        let err = b.apply_delta(102, 102, &[]).unwrap_err();
        assert_eq!(
            err,
            BookError::SequenceGap {
                expected: 101,
                got: 102
            }
        );
        assert!(!b.is_synced());
        assert_eq!(b.mid(), None);
        assert_eq!(b.apply_delta(103, 103, &[]), Err(BookError::NotSynced));
        b.apply_snapshot(200, &[l(99, 1)], &[l(100, 1)]).unwrap();
        assert!(b.is_synced());
    }

    #[test]
    fn crossed_book_is_rejected() {
        let mut b = book();
        let err = b
            .apply_delta(101, 101, &[(Side::Bid, l(101, 1))])
            .unwrap_err();
        assert!(matches!(err, BookError::Crossed { .. }));
        assert!(!b.is_synced());
    }

    #[test]
    fn impact_price_walks_levels() {
        let b = book();
        // Buy 3: 1 @ 101 + 2 @ 102 = 305 / 3
        assert_eq!(
            b.impact_price(Fx::from_int(3)),
            Some(Fx::from_int(305).checked_div(Fx::from_int(3)).unwrap())
        );
        // Sell 1: hits 99
        assert_eq!(b.impact_price(Fx::from_int(-1)), Some(Fx::from_int(99)));
        // More than the visible book.
        assert_eq!(b.impact_price(Fx::from_int(100)), None);
    }
}
