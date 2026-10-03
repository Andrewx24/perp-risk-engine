//! Positions, market parameters and cross-margin accounting.

use crate::fixed::Fx;
use crate::types::MarketId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketConfig {
    pub id: MarketId,
    pub symbol: String,
    /// Equity required to open or increase risk, in bps of notional.
    pub initial_margin_bps: i64,
    /// Equity below which the account is liquidated, in bps of notional.
    pub maintenance_margin_bps: i64,
    /// Penalty charged on liquidated notional, paid to the insurance fund.
    pub liquidation_fee_bps: i64,
    /// Fraction of a position closed per liquidation step.
    pub liquidation_chunk_bps: i64,
    /// Fills further than this from mark are rejected as bad prints.
    pub price_band_bps: i64,
}

impl MarketConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.maintenance_margin_bps <= 0 {
            return Err("maintenance margin must be positive");
        }
        if self.initial_margin_bps <= self.maintenance_margin_bps {
            return Err("initial margin must exceed maintenance margin");
        }
        // Closing a position at mark lowers the requirement by MMR * notional
        // and equity by fee * notional. If fee >= MMR, liquidating makes the
        // account *less* healthy and the loop never converges.
        if self.liquidation_fee_bps >= self.maintenance_margin_bps || self.liquidation_fee_bps < 0 {
            return Err("liquidation fee must be in [0, maintenance margin)");
        }
        if !(1..=10_000).contains(&self.liquidation_chunk_bps) {
            return Err("liquidation chunk must be in 1..=10000 bps");
        }
        if self.price_band_bps <= 0 {
            return Err("price band must be positive");
        }
        Ok(())
    }
}

/// A signed position: positive size is long.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Position {
    pub size: Fx,
    pub entry_price: Fx,
}

impl Position {
    pub fn notional(&self, mark: Fx) -> Fx {
        self.size.abs().mul_trunc(mark)
    }

    pub fn unrealized_pnl(&self, mark: Fx) -> Fx {
        self.size.mul_trunc(mark - self.entry_price)
    }

    /// Applies a fill of signed `qty` at `price`; returns realized PnL.
    ///
    /// Increasing moves the entry to the size-weighted average. Reducing
    /// realizes PnL on the closed part and keeps the entry. Flipping through
    /// zero realizes the whole old position and opens the remainder at
    /// `price`.
    pub fn apply_fill(&mut self, qty: Fx, price: Fx) -> Fx {
        if qty.is_zero() {
            return Fx::ZERO;
        }
        let old = self.size;
        let new = old + qty;
        if old.is_zero() || old.signum() == qty.signum() {
            let cost = old.abs().mul_trunc(self.entry_price) + qty.abs().mul_trunc(price);
            self.entry_price = cost.checked_div(new.abs()).unwrap_or(price);
            self.size = new;
            return Fx::ZERO;
        }
        let closed = if qty.abs() < old.abs() {
            qty.abs()
        } else {
            old.abs()
        };
        let closed_signed = if old.is_positive() { closed } else { -closed };
        let realized = closed_signed.mul_trunc(price - self.entry_price);
        self.size = new;
        if new.is_zero() {
            self.entry_price = Fx::ZERO;
        } else if new.signum() != old.signum() {
            self.entry_price = price;
        }
        realized
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Account {
    pub collateral: Fx,
    pub positions: BTreeMap<MarketId, Position>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct MarginSummary {
    pub collateral: Fx,
    pub unrealized_pnl: Fx,
    pub equity: Fx,
    pub notional: Fx,
    pub initial_requirement: Fx,
    pub maintenance_requirement: Fx,
}

impl MarginSummary {
    pub fn is_liquidatable(&self) -> bool {
        self.maintenance_requirement.is_positive() && self.equity < self.maintenance_requirement
    }

    pub fn free_collateral(&self) -> Fx {
        self.equity - self.initial_requirement
    }
}

impl Account {
    /// Cross-margin: one pool of equity backs every position.
    ///
    /// Panics if a position exists in a market with no mark. The engine only
    /// opens positions in markets with a mark, and marks are never removed.
    pub fn margin(
        &self,
        markets: &BTreeMap<MarketId, MarketConfig>,
        marks: &BTreeMap<MarketId, Fx>,
    ) -> MarginSummary {
        let mut s = MarginSummary {
            collateral: self.collateral,
            ..Default::default()
        };
        for (m, p) in &self.positions {
            let cfg = &markets[m];
            let mark = marks[m];
            let notional = p.notional(mark);
            s.unrealized_pnl += p.unrealized_pnl(mark);
            s.notional += notional;
            s.initial_requirement += notional.bps_ceil(cfg.initial_margin_bps);
            s.maintenance_requirement += notional.bps_ceil(cfg.maintenance_margin_bps);
        }
        s.equity = s.collateral + s.unrealized_pnl;
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fx(s: &str) -> Fx {
        s.parse().unwrap()
    }

    #[test]
    fn increase_averages_entry() {
        let mut p = Position::default();
        assert_eq!(p.apply_fill(fx("1"), fx("100")), Fx::ZERO);
        assert_eq!(p.apply_fill(fx("1"), fx("110")), Fx::ZERO);
        assert_eq!(p.size, fx("2"));
        assert_eq!(p.entry_price, fx("105"));
    }

    #[test]
    fn reduce_realizes_and_keeps_entry() {
        let mut p = Position {
            size: fx("2"),
            entry_price: fx("100"),
        };
        assert_eq!(p.apply_fill(fx("-0.5"), fx("120")), fx("10"));
        assert_eq!(p.size, fx("1.5"));
        assert_eq!(p.entry_price, fx("100"));
    }

    #[test]
    fn short_profits_when_price_falls() {
        let mut p = Position {
            size: fx("-3"),
            entry_price: fx("50"),
        };
        assert_eq!(p.unrealized_pnl(fx("40")), fx("30"));
        assert_eq!(p.apply_fill(fx("3"), fx("40")), fx("30"));
        assert_eq!(p, Position::default());
    }

    #[test]
    fn flip_realizes_all_and_reopens_at_fill() {
        let mut p = Position {
            size: fx("1"),
            entry_price: fx("100"),
        };
        assert_eq!(p.apply_fill(fx("-3"), fx("90")), fx("-10"));
        assert_eq!(p.size, fx("-2"));
        assert_eq!(p.entry_price, fx("90"));
    }

    #[test]
    fn fee_at_or_above_mmr_is_invalid() {
        let mut c = MarketConfig {
            id: MarketId(1),
            symbol: "X".into(),
            initial_margin_bps: 500,
            maintenance_margin_bps: 250,
            liquidation_fee_bps: 250,
            liquidation_chunk_bps: 2_500,
            price_band_bps: 500,
        };
        assert!(c.validate().is_err());
        c.liquidation_fee_bps = 100;
        assert!(c.validate().is_ok());
    }
}
