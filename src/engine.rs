//! The risk engine: a deterministic state machine over an ordered command log.
//!
//! `apply(command) -> events` is a pure function of the prior state. There is
//! no clock, no randomness, no `HashMap` iteration order and no floating
//! point inside it. Two replicas fed the same log reach the same state and
//! the same [`Engine::transcript_hash`] after every command, which is what
//! lets a follower verify a leader (or a restarted process verify its WAL)
//! by comparing one `u64` per sequence number.
//!
//! Liquidation runs inside the state machine, not beside it: a mark update
//! that pushes an account under maintenance produces its `Liquidated` events
//! in the same transition. There is no window where the engine has accepted
//! a price but not yet acted on it.

use crate::fixed::Fx;
use crate::margin::{Account, MarginSummary, MarketConfig, Position};
use crate::types::{AccountId, MarketId};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

/// The protocol's backstop vault. It takes over liquidated positions at mark
/// (as Hyperliquid's HLP or a dYdX-style backstop does) so the engine never
/// depends on venue liquidity being there at the worst possible moment.
/// Unwinding the vault's inventory on venues is an execution concern outside
/// this state machine. The vault is never itself liquidated.
pub const BACKSTOP: AccountId = AccountId(0);

/// Below this residual notional a liquidation closes the whole position
/// rather than leaving dust that costs more to track than it is worth.
const DUST_NOTIONAL: Fx = Fx::from_int(10);

/// Hard bound on liquidation steps per account per transition. Convergence is
/// guaranteed by `MarketConfig::validate`; this is the belt to that braces.
const MAX_LIQUIDATION_STEPS: usize = 256;

/// Trigger prices are padded outward by this much so that rounding in the
/// closed-form threshold can only produce false positives (an exact check
/// that finds nothing), never a missed liquidation.
const TRIGGER_PAD_BPS: i64 = 10;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    CreateMarket(MarketConfig),
    Deposit {
        account: AccountId,
        amount: Fx,
    },
    Withdraw {
        account: AccountId,
        amount: Fx,
    },
    /// A fill reported by the matching layer, to be risk-checked and booked.
    Trade {
        account: AccountId,
        market: MarketId,
        qty: Fx,
        price: Fx,
    },
    /// A consensus mark price from the oracle.
    Mark {
        market: MarketId,
        price: Fx,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    MarketCreated {
        market: MarketId,
    },
    Deposited {
        account: AccountId,
        amount: Fx,
        collateral: Fx,
    },
    Withdrawn {
        account: AccountId,
        amount: Fx,
        collateral: Fx,
    },
    TradeExecuted {
        account: AccountId,
        market: MarketId,
        qty: Fx,
        price: Fx,
        realized_pnl: Fx,
        position: Position,
    },
    Rejected {
        account: Option<AccountId>,
        reason: Reject,
    },
    MarkUpdated {
        market: MarketId,
        price: Fx,
    },
    Liquidated {
        account: AccountId,
        market: MarketId,
        qty: Fx,
        price: Fx,
        penalty: Fx,
    },
    BadDebt {
        account: AccountId,
        amount: Fx,
        insurance_covered: Fx,
        socialized: Fx,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Reject {
    InvalidMarket { detail: String },
    UnknownMarket { market: MarketId },
    UnknownAccount,
    NoMarkPrice { market: MarketId },
    InvalidAmount,
    PriceOutOfBand { price: Fx, mark: Fx },
    InsufficientMargin { required: Fx, available: Fx },
}

/// Where an account sits in the liquidation trigger index.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Trigger {
    /// One long position: can only become liquidatable at mark <= price.
    Below(MarketId, Fx),
    /// One short position: can only become liquidatable at mark >= price.
    Above(MarketId, Fx),
    /// Several positions. Each market gets a price band around the mark at
    /// which the account was last checked; while every mark stays strictly
    /// inside its band the account provably cannot be liquidatable. Leaving
    /// any band triggers an exact check, which re-centres the bands.
    Band(Vec<(MarketId, Fx, Fx)>),
}

/// Per-market liquidation candidates ordered by trigger price, so a mark
/// update visits only the accounts it can actually affect —
/// O(log n + k) instead of re-margining every exposed account.
#[derive(Clone, Debug, Default)]
struct TriggerIndex {
    /// Check the account when the mark is at or below the key.
    below: BTreeSet<(Fx, AccountId)>,
    /// Check the account when the mark is at or above the key.
    above: BTreeSet<(Fx, AccountId)>,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct EngineStats {
    pub liquidations: u64,
    pub rejected: u64,
    pub bad_debt: Fx,
    pub socialized: Fx,
}

#[derive(Clone, Debug, Serialize)]
pub struct PositionView {
    pub market: MarketId,
    pub size: Fx,
    pub entry_price: Fx,
    pub mark: Fx,
    pub unrealized_pnl: Fx,
}

#[derive(Clone, Debug, Serialize)]
pub struct AccountView {
    pub account: AccountId,
    pub margin: MarginSummary,
    pub liquidatable: bool,
    pub positions: Vec<PositionView>,
}

/// FNV-1a over little-endian encodings, so the transcript hash is identical
/// across platforms (std's default `Hasher::write_u64` uses native endianness,
/// and `SipHash` is randomly keyed per process).
struct Fnv(u64);

impl Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= *b as u64;
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    fn write_u8(&mut self, i: u8) {
        self.write(&[i]);
    }
    fn write_u32(&mut self, i: u32) {
        self.write(&i.to_le_bytes());
    }
    fn write_u64(&mut self, i: u64) {
        self.write(&i.to_le_bytes());
    }
    fn write_i64(&mut self, i: i64) {
        self.write(&i.to_le_bytes());
    }
    fn write_usize(&mut self, i: usize) {
        self.write(&(i as u64).to_le_bytes());
    }
    fn write_isize(&mut self, i: isize) {
        self.write(&(i as i64).to_le_bytes());
    }
}

#[derive(Debug, Clone)]
pub struct Engine {
    markets: BTreeMap<MarketId, MarketConfig>,
    marks: BTreeMap<MarketId, Fx>,
    accounts: BTreeMap<AccountId, Account>,
    /// Which accounts hold a position in each market: a mark update only
    /// re-margins the accounts it can affect, not the whole book.
    exposure: BTreeMap<MarketId, BTreeSet<AccountId>>,
    insurance_fund: Fx,
    seq: u64,
    transcript: u64,
    stats: EngineStats,
    triggers: BTreeMap<MarketId, TriggerIndex>,
    trigger_of: BTreeMap<AccountId, Trigger>,
    scratch: Vec<AccountId>,
    /// Accounts whose collateral a loss socialization reduced. They may now
    /// be under maintenance in markets the triggering mark never touched, so
    /// they are re-checked before the transition ends.
    pending: Vec<AccountId>,
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            markets: BTreeMap::new(),
            marks: BTreeMap::new(),
            accounts: BTreeMap::new(),
            exposure: BTreeMap::new(),
            insurance_fund: Fx::ZERO,
            seq: 0,
            transcript: 0xcbf2_9ce4_8422_2325,
            stats: EngineStats::default(),
            triggers: BTreeMap::new(),
            trigger_of: BTreeMap::new(),
            scratch: Vec::new(),
            pending: Vec::new(),
        }
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn transcript_hash(&self) -> u64 {
        self.transcript
    }

    pub fn insurance_fund(&self) -> Fx {
        self.insurance_fund
    }

    pub fn stats(&self) -> EngineStats {
        self.stats
    }

    pub fn markets(&self) -> impl Iterator<Item = &MarketConfig> {
        self.markets.values()
    }

    pub fn mark(&self, m: MarketId) -> Option<Fx> {
        self.marks.get(&m).copied()
    }

    pub fn marks(&self) -> &BTreeMap<MarketId, Fx> {
        &self.marks
    }

    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    pub fn account(&self, id: AccountId) -> Option<&Account> {
        self.accounts.get(&id)
    }

    pub fn margin(&self, id: AccountId) -> Option<MarginSummary> {
        self.accounts
            .get(&id)
            .map(|a| a.margin(&self.markets, &self.marks))
    }

    pub fn account_view(&self, id: AccountId) -> Option<AccountView> {
        let a = self.accounts.get(&id)?;
        let margin = a.margin(&self.markets, &self.marks);
        let positions = a
            .positions
            .iter()
            .map(|(m, p)| {
                let mark = self.marks[m];
                PositionView {
                    market: *m,
                    size: p.size,
                    entry_price: p.entry_price,
                    mark,
                    unrealized_pnl: p.unrealized_pnl(mark),
                }
            })
            .collect();
        Some(AccountView {
            account: id,
            liquidatable: id != BACKSTOP && margin.is_liquidatable(),
            margin,
            positions,
        })
    }

    /// Applies one command, appending the resulting events to `out`.
    /// `out` is caller-owned so the hot path can reuse one buffer.
    pub fn apply(&mut self, cmd: &Command, out: &mut Vec<Event>) {
        let start = out.len();
        match cmd {
            Command::CreateMarket(cfg) => self.create_market(cfg, out),
            Command::Deposit { account, amount } => self.deposit(*account, *amount, out),
            Command::Withdraw { account, amount } => self.withdraw(*account, *amount, out),
            Command::Trade {
                account,
                market,
                qty,
                price,
            } => self.trade(*account, *market, *qty, *price, out),
            Command::Mark { market, price } => self.mark_price(*market, *price, out),
        }
        self.seq += 1;
        let mut h = Fnv(self.transcript);
        self.seq.hash(&mut h);
        for e in &out[start..] {
            if matches!(e, Event::Rejected { .. }) {
                self.stats.rejected += 1;
            }
            e.hash(&mut h);
        }
        self.transcript = h.finish();
    }

    fn reject(out: &mut Vec<Event>, account: Option<AccountId>, reason: Reject) {
        out.push(Event::Rejected { account, reason });
    }

    fn create_market(&mut self, cfg: &MarketConfig, out: &mut Vec<Event>) {
        if self.markets.contains_key(&cfg.id) {
            return Self::reject(
                out,
                None,
                Reject::InvalidMarket {
                    detail: "duplicate market id".into(),
                },
            );
        }
        if let Err(e) = cfg.validate() {
            return Self::reject(out, None, Reject::InvalidMarket { detail: e.into() });
        }
        self.markets.insert(cfg.id, cfg.clone());
        out.push(Event::MarketCreated { market: cfg.id });
    }

    fn deposit(&mut self, account: AccountId, amount: Fx, out: &mut Vec<Event>) {
        if !amount.is_positive() {
            return Self::reject(out, Some(account), Reject::InvalidAmount);
        }
        let a = self.accounts.entry(account).or_default();
        a.collateral += amount;
        let collateral = a.collateral;
        self.reindex(account);
        out.push(Event::Deposited {
            account,
            amount,
            collateral,
        });
    }

    fn withdraw(&mut self, account: AccountId, amount: Fx, out: &mut Vec<Event>) {
        if !amount.is_positive() {
            return Self::reject(out, Some(account), Reject::InvalidAmount);
        }
        let Some(a) = self.accounts.get(&account) else {
            return Self::reject(out, Some(account), Reject::UnknownAccount);
        };
        // Unrealized profit backs margin but cannot leave the system.
        let s = a.margin(&self.markets, &self.marks);
        let free = s.free_collateral().min(a.collateral).max(Fx::ZERO);
        if amount > free {
            return Self::reject(
                out,
                Some(account),
                Reject::InsufficientMargin {
                    required: amount,
                    available: free,
                },
            );
        }
        let a = self.accounts.get_mut(&account).expect("checked above");
        a.collateral -= amount;
        let collateral = a.collateral;
        self.reindex(account);
        out.push(Event::Withdrawn {
            account,
            amount,
            collateral,
        });
    }

    fn trade(
        &mut self,
        account: AccountId,
        market: MarketId,
        qty: Fx,
        price: Fx,
        out: &mut Vec<Event>,
    ) {
        if qty.is_zero() || !price.is_positive() {
            return Self::reject(out, Some(account), Reject::InvalidAmount);
        }
        let Some(cfg) = self.markets.get(&market) else {
            return Self::reject(out, Some(account), Reject::UnknownMarket { market });
        };
        let Some(&mark) = self.marks.get(&market) else {
            return Self::reject(out, Some(account), Reject::NoMarkPrice { market });
        };
        if (price - mark).abs() > mark.bps(cfg.price_band_bps) {
            return Self::reject(out, Some(account), Reject::PriceOutOfBand { price, mark });
        }
        let Some(current) = self.accounts.get(&account) else {
            return Self::reject(out, Some(account), Reject::UnknownAccount);
        };

        // Pre-trade check against the post-trade state. Positions per account
        // are few, so cloning the account is cheaper than an undo log.
        let mut next = current.clone();
        let old = next.positions.get(&market).copied().unwrap_or_default();
        let pos = next.positions.entry(market).or_default();
        let realized = pos.apply_fill(qty, price);
        let position = *pos;
        if position.size.is_zero() {
            next.positions.remove(&market);
        }
        next.collateral += realized;

        let reduces_risk = position.size.abs() <= old.size.abs()
            && position.size.signum() * old.size.signum() >= 0;
        if !reduces_risk && account != BACKSTOP {
            let s = next.margin(&self.markets, &self.marks);
            if s.equity < s.initial_requirement {
                return Self::reject(
                    out,
                    Some(account),
                    Reject::InsufficientMargin {
                        required: s.initial_requirement,
                        available: s.equity,
                    },
                );
            }
        }

        self.accounts.insert(account, next);
        self.track_exposure(account, market, position.size);
        self.reindex(account);
        out.push(Event::TradeExecuted {
            account,
            market,
            qty,
            price,
            realized_pnl: realized,
            position,
        });
        // A reducing fill inside the band can still leave the account under
        // maintenance (it realized a loss at a worse-than-mark price).
        self.liquidate_if_needed(account, out);
        self.drain_cascade(out);
    }

    fn mark_price(&mut self, market: MarketId, price: Fx, out: &mut Vec<Event>) {
        if !self.markets.contains_key(&market) {
            return Self::reject(out, None, Reject::UnknownMarket { market });
        }
        if !price.is_positive() {
            return Self::reject(out, None, Reject::InvalidAmount);
        }
        self.marks.insert(market, price);
        out.push(Event::MarkUpdated { market, price });

        let mut ids = std::mem::take(&mut self.scratch);
        ids.clear();
        if let Some(ix) = self.triggers.get(&market) {
            ids.extend(ix.below.range((price, AccountId(0))..).map(|(_, id)| *id));
            ids.extend(
                ix.above
                    .range(..=(price, AccountId(u64::MAX)))
                    .map(|(_, id)| *id),
            );
        }
        ids.sort_unstable();
        ids.dedup();
        for &id in &ids {
            self.liquidate_if_needed(id, out);
            // Survivors of an exact check get bands re-centred on this mark.
            self.reindex(id);
        }
        self.scratch = ids;
        self.drain_cascade(out);
    }

    /// Liquidates accounts pushed under maintenance by a socialized loss,
    /// which may itself socialize further. Terminates: every round removes
    /// positions or exhausts the collateral a haircut can take.
    fn drain_cascade(&mut self, out: &mut Vec<Event>) {
        while let Some(id) = self.pending.pop() {
            self.reindex(id);
            self.liquidate_if_needed(id, out);
        }
    }

    /// Computes where an account belongs in the trigger index.
    ///
    /// **One position** — a closed-form liquidation price, padded outward.
    /// With collateral C, size s, entry e and maintenance rate r:
    ///
    /// - long:  C + s(p - e) < s·p·r   ⇔  p < (e - C/s) / (1 - r)
    /// - short: C - s(p - e) < s·p·r   ⇔  p > (C/s + e) / (1 + r)   (s = |size|)
    ///
    /// Rounding in the margin arithmetic moves the true boundary by a few
    /// micro-dollars, i.e. by `δ / s` in price, so the pad is
    /// `TRIGGER_PAD_BPS` of the price plus `8 µ$ / s`.
    ///
    /// **Several positions** — the boundary in one market moves with every
    /// other market's mark, so there is no fixed price. But
    /// `f = equity - maintenance` is separable across markets, and a move of
    /// Δp in market m changes it by at most `|s_m|·(1 + r_m)·|Δp|`. Give every
    /// market the same relative tolerance `k = (f - ε) / (N + MM)`, where N is
    /// total notional and MM the (rounded-up) requirement: then
    /// `Σ |s_m|(1 + r_m)·p_m·k ≤ f - ε`, so while every mark is strictly
    /// within `p_m·(1 ± k)` of where it was, f stays positive.
    ///
    /// Anything that cannot be computed without overflow degrades to a
    /// zero-width band: checked on every mark.
    fn compute_trigger(&self, acct: &Account) -> Option<Trigger> {
        let mut it = acct.positions.iter();
        let (&m, p) = it.next()?;
        if it.next().is_some() {
            return Some(self.compute_band(acct));
        }
        let rate = Fx::from_raw(self.markets[&m].maintenance_margin_bps * 100);
        let s = p.size.abs();
        let c = acct.collateral;
        let slack = Fx::from_raw(8).checked_div(s);
        let t = if p.size.is_positive() {
            p.entry_price
                .checked_sub(c.checked_div(s)?)
                .and_then(|n| n.checked_div(Fx::ONE - rate))
                .map(|t| t.max(Fx::ZERO))
                .and_then(|t| {
                    t.checked_add(t.bps_ceil(TRIGGER_PAD_BPS))?
                        .checked_add(slack?)
                })
                .map(|t| Trigger::Below(m, t))
        } else {
            c.checked_div(s)
                .and_then(|x| x.checked_add(p.entry_price))
                .and_then(|n| n.checked_div(Fx::ONE + rate))
                .and_then(|t| {
                    t.checked_sub(t.bps_ceil(TRIGGER_PAD_BPS))?
                        .checked_sub(slack?)
                })
                .map(|t| Trigger::Above(m, t.max(Fx::ZERO)))
        };
        Some(t.unwrap_or_else(|| {
            let mark = self.marks[&m];
            Trigger::Band(vec![(m, mark, mark)])
        }))
    }

    fn compute_band(&self, acct: &Account) -> Trigger {
        let s = acct.margin(&self.markets, &self.marks);
        let eps = Fx::from_raw(8 * acct.positions.len() as i64);
        let k = (s.equity - s.maintenance_requirement - eps)
            .max(Fx::ZERO)
            .checked_div(s.notional + s.maintenance_requirement)
            .unwrap_or(Fx::ZERO);
        Trigger::Band(
            acct.positions
                .keys()
                .map(|m| {
                    let mark = self.marks[m];
                    // Truncation narrows the band: conservative.
                    let d = mark.mul_trunc(k);
                    (*m, (mark - d).max(Fx::ZERO), mark + d)
                })
                .collect(),
        )
    }

    /// Re-files an account in the trigger index after anything that changed
    /// its collateral or positions, or after an exact check (re-centring a
    /// band).
    fn reindex(&mut self, id: AccountId) {
        if let Some(old) = self.trigger_of.remove(&id) {
            match old {
                Trigger::Below(m, t) => {
                    self.triggers.entry(m).or_default().below.remove(&(t, id));
                }
                Trigger::Above(m, t) => {
                    self.triggers.entry(m).or_default().above.remove(&(t, id));
                }
                Trigger::Band(bands) => {
                    for (m, lo, hi) in bands {
                        let ix = self.triggers.entry(m).or_default();
                        ix.below.remove(&(lo, id));
                        ix.above.remove(&(hi, id));
                    }
                }
            }
        }
        if id == BACKSTOP {
            return;
        }
        let Some(trigger) = self.accounts.get(&id).and_then(|a| self.compute_trigger(a)) else {
            return;
        };
        match &trigger {
            Trigger::Below(m, t) => {
                self.triggers.entry(*m).or_default().below.insert((*t, id));
            }
            Trigger::Above(m, t) => {
                self.triggers.entry(*m).or_default().above.insert((*t, id));
            }
            Trigger::Band(bands) => {
                for (m, lo, hi) in bands {
                    let ix = self.triggers.entry(*m).or_default();
                    ix.below.insert((*lo, id));
                    ix.above.insert((*hi, id));
                }
            }
        }
        self.trigger_of.insert(id, trigger);
    }

    fn track_exposure(&mut self, account: AccountId, market: MarketId, size: Fx) {
        let set = self.exposure.entry(market).or_default();
        if size.is_zero() {
            set.remove(&account);
        } else {
            set.insert(account);
        }
    }

    /// Partial liquidation in chunks, largest maintenance contributor first,
    /// until the account is back above maintenance or flat. Each chunk is
    /// transferred to the backstop at mark; the penalty goes to the
    /// insurance fund. A flat account left with negative collateral is bad
    /// debt.
    fn liquidate_if_needed(&mut self, id: AccountId, out: &mut Vec<Event>) {
        if id == BACKSTOP {
            return;
        }
        for step in 0..=MAX_LIQUIDATION_STEPS {
            let Some(acct) = self.accounts.get(&id) else {
                return;
            };
            let summary = acct.margin(&self.markets, &self.marks);
            if !summary.is_liquidatable() {
                break;
            }
            let (market, pos) = acct
                .positions
                .iter()
                .max_by_key(|(m, p)| {
                    p.notional(self.marks[*m])
                        .bps_ceil(self.markets[*m].maintenance_margin_bps)
                })
                .map(|(m, p)| (*m, *p))
                .expect("liquidatable implies a position");
            let cfg = &self.markets[&market];
            let mark = self.marks[&market];

            let mut close = pos.size.abs().bps_ceil(cfg.liquidation_chunk_bps);
            if close.is_zero()
                || close > pos.size.abs()
                || (pos.size.abs() - close).mul_trunc(mark) < DUST_NOTIONAL
                || step == MAX_LIQUIDATION_STEPS
                // Already bankrupt: chunking only delays the inevitable.
                || !summary.equity.is_positive()
            {
                close = pos.size.abs();
            }
            let qty = if pos.size.is_positive() {
                -close
            } else {
                close
            };
            let fee_bps = cfg.liquidation_fee_bps;

            let acct = self.accounts.get_mut(&id).expect("exists");
            let p = acct.positions.get_mut(&market).expect("exists");
            let realized = p.apply_fill(qty, mark);
            let remaining = p.size;
            if remaining.is_zero() {
                acct.positions.remove(&market);
            }
            acct.collateral += realized;
            let equity = acct.margin(&self.markets, &self.marks).equity;
            // Never charge a penalty out of money the account no longer has:
            // that would just be the insurance fund paying itself.
            let penalty = close
                .mul_trunc(mark)
                .bps_ceil(fee_bps)
                .min(equity.max(Fx::ZERO));
            let acct = self.accounts.get_mut(&id).expect("exists");
            acct.collateral -= penalty;
            self.insurance_fund += penalty;
            self.track_exposure(id, market, remaining);

            let vault = self.accounts.entry(BACKSTOP).or_default();
            let vp = vault.positions.entry(market).or_default();
            let vault_realized = vp.apply_fill(-qty, mark);
            let vault_size = vp.size;
            if vault_size.is_zero() {
                vault.positions.remove(&market);
            }
            vault.collateral += vault_realized;
            self.track_exposure(BACKSTOP, market, vault_size);
            self.reindex(id);

            self.stats.liquidations += 1;
            out.push(Event::Liquidated {
                account: id,
                market,
                qty,
                price: mark,
                penalty,
            });
        }

        let acct = self.accounts.get_mut(&id).expect("exists");
        if acct.positions.is_empty() && acct.collateral.is_negative() {
            let debt = -acct.collateral;
            acct.collateral = Fx::ZERO;
            self.reindex(id);
            let covered = debt.min(self.insurance_fund);
            self.insurance_fund -= covered;
            let socialized = debt - covered;
            if socialized.is_positive() {
                self.socialize(id, socialized);
            }
            self.stats.bad_debt += debt;
            self.stats.socialized += socialized;
            out.push(Event::BadDebt {
                account: id,
                amount: debt,
                insurance_covered: covered,
                socialized,
            });
        }
    }

    /// Spreads a loss the insurance fund could not cover across accounts
    /// with positive collateral, pro rata. Truncation dust goes to the
    /// largest balance so the haircut sums exactly to the loss.
    fn socialize(&mut self, bankrupt: AccountId, loss: Fx) {
        let eligible = |id: &AccountId, a: &Account| {
            *id != bankrupt && *id != BACKSTOP && a.collateral.is_positive()
        };
        let total: Fx = self
            .accounts
            .iter()
            .filter(|(id, a)| eligible(id, a))
            .map(|(_, a)| a.collateral)
            .sum();
        // Whatever the solvent accounts cannot absorb, the vault carries as a
        // deficit. It is recorded, never silently dropped.
        let absorbable = loss.min(total.max(Fx::ZERO));
        if loss > absorbable {
            self.accounts.entry(BACKSTOP).or_default().collateral -= loss - absorbable;
        }
        if !absorbable.is_positive() {
            return;
        }
        let loss = absorbable;
        let mut assigned = Fx::ZERO;
        let mut largest: Option<(AccountId, Fx)> = None;
        for (id, a) in self.accounts.iter_mut() {
            if !eligible(id, a) {
                continue;
            }
            let share = Fx::from_raw(
                (loss.raw() as i128 * a.collateral.raw() as i128 / total.raw() as i128) as i64,
            );
            a.collateral -= share;
            assigned += share;
            self.pending.push(*id);
            if largest.is_none_or(|(_, c)| a.collateral > c) {
                largest = Some((*id, a.collateral));
            }
        }
        if let Some((id, _)) = largest {
            let a = self.accounts.get_mut(&id).expect("exists");
            a.collateral -= loss - assigned;
            if !self.pending.contains(&id) {
                self.pending.push(id);
            }
        }
    }

    /// Structural invariants. Cheap enough to run after every command in
    /// tests; the property tests do exactly that.
    pub fn check_invariants(&self) -> Result<(), String> {
        if self.insurance_fund.is_negative() {
            return Err(format!("insurance fund negative: {}", self.insurance_fund));
        }
        for (id, a) in &self.accounts {
            for (m, p) in &a.positions {
                if p.size.is_zero() {
                    return Err(format!("{id} holds a zero position in {m}"));
                }
                if !self.exposure.get(m).is_some_and(|s| s.contains(id)) {
                    return Err(format!("{id} missing from exposure index for {m}"));
                }
            }
            if *id != BACKSTOP && a.positions.is_empty() && a.collateral.is_negative() {
                return Err(format!(
                    "{id} is flat with negative collateral {}",
                    a.collateral
                ));
            }
        }
        for (id, a) in &self.accounts {
            let want = if *id == BACKSTOP {
                None
            } else {
                self.compute_trigger(a)
            };
            let have = self.trigger_of.get(id);
            match (have, &want) {
                // Bands are centred on the marks of the last exact check, so
                // they legitimately differ from a fresh computation. What
                // must hold is soundness: inside every band, not liquidatable.
                (Some(Trigger::Band(bands)), Some(Trigger::Band(_))) => {
                    let inside = bands.iter().all(|(m, lo, hi)| {
                        let p = self.marks[m];
                        *lo < p && p < *hi
                    });
                    let same_markets = bands.iter().map(|b| b.0).eq(a.positions.keys().copied());
                    if !same_markets {
                        return Err(format!("{id} band covers the wrong markets"));
                    }
                    if inside && a.margin(&self.markets, &self.marks).is_liquidatable() {
                        return Err(format!("{id} liquidatable inside its bands {bands:?}"));
                    }
                }
                (h, w) if h == w.as_ref() => {}
                (h, w) => return Err(format!("{id} trigger stale: have {h:?}, want {w:?}")),
            }
        }
        for (m, set) in &self.exposure {
            for id in set {
                if !self
                    .accounts
                    .get(id)
                    .is_some_and(|a| a.positions.contains_key(m))
                {
                    return Err(format!(
                        "exposure index lists {id} in {m} without a position"
                    ));
                }
            }
        }
        Ok(())
    }

    /// Every exposed non-backstop account is above maintenance. Holds after
    /// any `Mark` command, because liquidation runs inside that transition.
    pub fn check_no_liquidatable(&self) -> Result<(), String> {
        for (id, a) in &self.accounts {
            if *id != BACKSTOP && a.margin(&self.markets, &self.marks).is_liquidatable() {
                return Err(format!("{id} left liquidatable"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fx(s: &str) -> Fx {
        s.parse().unwrap()
    }

    const BTC: MarketId = MarketId(1);
    const ALICE: AccountId = AccountId(1);
    const BOB: AccountId = AccountId(2);

    fn btc() -> MarketConfig {
        MarketConfig {
            id: BTC,
            symbol: "BTC-PERP".into(),
            initial_margin_bps: 1_000,
            maintenance_margin_bps: 500,
            liquidation_fee_bps: 100,
            liquidation_chunk_bps: 2_500,
            price_band_bps: 1_000,
        }
    }

    fn run(e: &mut Engine, cmds: &[Command]) -> Vec<Event> {
        let mut out = Vec::new();
        for c in cmds {
            e.apply(c, &mut out);
            e.check_invariants().unwrap();
        }
        out
    }

    fn setup() -> Engine {
        let mut e = Engine::new();
        run(
            &mut e,
            &[
                Command::CreateMarket(btc()),
                Command::Mark {
                    market: BTC,
                    price: fx("100"),
                },
                Command::Deposit {
                    account: ALICE,
                    amount: fx("100"),
                },
                Command::Deposit {
                    account: BOB,
                    amount: fx("10000"),
                },
            ],
        );
        e
    }

    #[test]
    fn initial_margin_is_enforced_on_open() {
        let mut e = setup();
        // 10% IM: 100 equity supports 1,000 notional = 10 units at 100.
        let ev = run(
            &mut e,
            &[Command::Trade {
                account: ALICE,
                market: BTC,
                qty: fx("10.01"),
                price: fx("100"),
            }],
        );
        assert!(matches!(
            ev[0],
            Event::Rejected {
                reason: Reject::InsufficientMargin { .. },
                ..
            }
        ));
        let ev = run(
            &mut e,
            &[Command::Trade {
                account: ALICE,
                market: BTC,
                qty: fx("10"),
                price: fx("100"),
            }],
        );
        assert!(matches!(ev[0], Event::TradeExecuted { .. }));
    }

    #[test]
    fn reducing_trades_are_always_allowed() {
        let mut e = setup();
        run(
            &mut e,
            &[Command::Trade {
                account: ALICE,
                market: BTC,
                qty: fx("10"),
                price: fx("100"),
            }],
        );
        // Price drops 4%: equity 60, IM requirement 96 — cannot open, can close.
        run(
            &mut e,
            &[Command::Mark {
                market: BTC,
                price: fx("96"),
            }],
        );
        let ev = run(
            &mut e,
            &[Command::Trade {
                account: ALICE,
                market: BTC,
                qty: fx("1"),
                price: fx("96"),
            }],
        );
        assert!(matches!(ev[0], Event::Rejected { .. }));
        let ev = run(
            &mut e,
            &[Command::Trade {
                account: ALICE,
                market: BTC,
                qty: fx("-1"),
                price: fx("96"),
            }],
        );
        assert!(matches!(ev[0], Event::TradeExecuted { .. }));
    }

    #[test]
    fn fills_outside_the_band_are_bad_prints() {
        let mut e = setup();
        let ev = run(
            &mut e,
            &[Command::Trade {
                account: BOB,
                market: BTC,
                qty: fx("1"),
                price: fx("111"),
            }],
        );
        assert!(matches!(
            ev[0],
            Event::Rejected {
                reason: Reject::PriceOutOfBand { .. },
                ..
            }
        ));
    }

    #[test]
    fn mark_drop_liquidates_partially_and_restores_health() {
        let mut e = setup();
        run(
            &mut e,
            &[Command::Trade {
                account: ALICE,
                market: BTC,
                qty: fx("10"),
                price: fx("100"),
            }],
        );
        // At 95.5: equity 55, MM 5% of 955 = 47.75 — still healthy.
        let ev = run(
            &mut e,
            &[Command::Mark {
                market: BTC,
                price: fx("95.5"),
            }],
        );
        assert_eq!(ev.len(), 1);
        // At 95: equity 50, MM 47.5 — healthy. At 94.5: equity 45 < 47.25.
        let ev = run(
            &mut e,
            &[Command::Mark {
                market: BTC,
                price: fx("94.5"),
            }],
        );
        let liqs: Vec<_> = ev
            .iter()
            .filter(|e| matches!(e, Event::Liquidated { .. }))
            .collect();
        assert_eq!(liqs.len(), 1, "one 25% chunk is enough: {ev:?}");
        let s = e.margin(ALICE).unwrap();
        assert!(!s.is_liquidatable());
        assert_eq!(e.account(ALICE).unwrap().positions[&BTC].size, fx("7.5"));
        assert_eq!(e.account(BACKSTOP).unwrap().positions[&BTC].size, fx("2.5"));
        assert!(e.insurance_fund().is_positive());
        e.check_no_liquidatable().unwrap();
    }

    #[test]
    fn gap_through_bankruptcy_uses_insurance_then_socializes() {
        let mut e = setup();
        run(
            &mut e,
            &[Command::Trade {
                account: ALICE,
                market: BTC,
                qty: fx("10"),
                price: fx("100"),
            }],
        );
        // 20% gap down: equity -100. Fully closed, 100 of bad debt, empty fund.
        let ev = run(
            &mut e,
            &[Command::Mark {
                market: BTC,
                price: fx("80"),
            }],
        );
        let bad = ev.iter().find_map(|e| match e {
            Event::BadDebt {
                amount,
                insurance_covered,
                socialized,
                ..
            } => Some((*amount, *insurance_covered, *socialized)),
            _ => None,
        });
        assert_eq!(bad, Some((fx("100"), Fx::ZERO, fx("100"))));
        assert!(e.account(ALICE).unwrap().positions.is_empty());
        assert_eq!(e.account(ALICE).unwrap().collateral, Fx::ZERO);
        // Bob was the only solvent account: he absorbs the loss.
        assert_eq!(e.account(BOB).unwrap().collateral, fx("9900"));
    }

    #[test]
    fn socialized_loss_that_breaks_another_market_is_liquidated_in_the_same_step() {
        // Found by the property tests: a haircut from BTC bad debt pushed an
        // account that only held ETH under maintenance, and nothing re-checked
        // it because the triggering mark was for BTC.
        let eth = MarketConfig {
            id: MarketId(2),
            symbol: "ETH-PERP".into(),
            ..btc()
        };
        let mut e = Engine::new();
        run(
            &mut e,
            &[
                Command::CreateMarket(btc()),
                Command::CreateMarket(eth),
                Command::Mark {
                    market: BTC,
                    price: fx("100"),
                },
                Command::Mark {
                    market: MarketId(2),
                    price: fx("100"),
                },
                Command::Deposit {
                    account: ALICE,
                    amount: fx("100"),
                },
                Command::Deposit {
                    account: BOB,
                    amount: fx("100"),
                },
                Command::Trade {
                    account: ALICE,
                    market: BTC,
                    qty: fx("10"),
                    price: fx("100"),
                },
                Command::Trade {
                    account: BOB,
                    market: MarketId(2),
                    qty: fx("9.5"),
                    price: fx("100"),
                },
            ],
        );
        let ev = run(
            &mut e,
            &[Command::Mark {
                market: BTC,
                price: fx("80"),
            }],
        );
        assert!(
            ev.iter()
                .any(|e| matches!(e, Event::BadDebt { account: ALICE, .. }))
        );
        assert!(
            ev.iter().any(|e| matches!(
                e,
                Event::Liquidated {
                    account: BOB,
                    market: MarketId(2),
                    ..
                }
            )),
            "{ev:?}"
        );
        e.check_no_liquidatable().unwrap();
    }

    #[test]
    fn withdraw_cannot_take_unrealized_profit_or_margin() {
        let mut e = setup();
        run(
            &mut e,
            &[
                Command::Trade {
                    account: ALICE,
                    market: BTC,
                    qty: fx("5"),
                    price: fx("100"),
                },
                Command::Mark {
                    market: BTC,
                    price: fx("120"),
                },
            ],
        );
        // collateral 100, upnl 100, equity 200, IM 60 → free 140, capped at collateral 100.
        let ev = run(
            &mut e,
            &[Command::Withdraw {
                account: ALICE,
                amount: fx("100.01"),
            }],
        );
        assert!(matches!(ev[0], Event::Rejected { .. }));
        let ev = run(
            &mut e,
            &[Command::Withdraw {
                account: ALICE,
                amount: fx("100"),
            }],
        );
        assert!(matches!(ev[0], Event::Withdrawn { .. }));
    }

    #[test]
    fn transcript_hash_is_deterministic_and_sensitive() {
        let cmds = [
            Command::CreateMarket(btc()),
            Command::Mark {
                market: BTC,
                price: fx("100"),
            },
            Command::Deposit {
                account: ALICE,
                amount: fx("100"),
            },
            Command::Trade {
                account: ALICE,
                market: BTC,
                qty: fx("10"),
                price: fx("100"),
            },
            Command::Mark {
                market: BTC,
                price: fx("90"),
            },
        ];
        let mut a = Engine::new();
        let mut b = Engine::new();
        run(&mut a, &cmds);
        run(&mut b, &cmds);
        assert_eq!(a.transcript_hash(), b.transcript_hash());

        let mut c = Engine::new();
        let mut tweaked = cmds.clone();
        tweaked[4] = Command::Mark {
            market: BTC,
            price: fx("90.000001"),
        };
        run(&mut c, &tweaked);
        assert_ne!(a.transcript_hash(), c.transcript_hash());
    }
}
