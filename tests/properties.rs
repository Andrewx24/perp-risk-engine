//! Property tests over arbitrary command sequences.
//!
//! The unit tests pin known scenarios; these hunt for the ones nobody thought
//! of. For any sequence of deposits, withdrawals, trades and (possibly
//! violent) mark moves:
//!
//! 1. structural invariants hold after every command;
//! 2. after every mark update, no account is left under maintenance;
//! 3. two engines fed the same log agree on the transcript hash at every step;
//! 4. the log survives a JSON round trip (what the WAL stores) unchanged.

use perp_risk_engine::{AccountId, Command, Engine, Fx, MarketId, default_markets};
use proptest::prelude::*;

const ACCOUNTS: u64 = 6;

fn market() -> impl Strategy<Value = MarketId> {
    (1u32..=3).prop_map(MarketId)
}

fn account() -> impl Strategy<Value = AccountId> {
    (1..=ACCOUNTS).prop_map(AccountId)
}

/// Commands expressed relative to the current mark, resolved at run time, so
/// trades land inside the price band often enough to be interesting.
#[derive(Clone, Debug)]
enum Op {
    Deposit(AccountId, i64),
    Withdraw(AccountId, i64),
    Trade(AccountId, MarketId, i64, u32, i64),
    Move(MarketId, i64),
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        2 => (account(), 1i64..100_000).prop_map(|(a, x)| Op::Deposit(a, x)),
        1 => (account(), 1i64..50_000).prop_map(|(a, x)| Op::Withdraw(a, x)),
        // qty mantissa scaled by 10^0..10^3 raw units: from 0.000001 (dust,
        // where rounding error in price terms is largest) up to 50 units;
        // price offset in bps of mark
        6 => (account(), market(), -50_000i64..50_000, 0u32..4, -120i64..120)
            .prop_map(|(a, m, q, k, o)| Op::Trade(a, m, q, k, o)),
        // mark moves up to ±30% in one step: gaps through bankruptcy
        4 => (market(), -3_000i64..3_000).prop_map(|(m, bps)| Op::Move(m, bps)),
    ]
}

fn genesis() -> Vec<Command> {
    let mut cmds: Vec<Command> = default_markets()
        .into_iter()
        .map(Command::CreateMarket)
        .collect();
    for (m, p) in [(1, 65_000), (2, 3_200), (3, 150)] {
        cmds.push(Command::Mark {
            market: MarketId(m),
            price: Fx::from_int(p),
        });
    }
    cmds
}

fn resolve(e: &Engine, op: &Op) -> Command {
    match *op {
        Op::Deposit(account, x) => Command::Deposit {
            account,
            amount: Fx::from_int(x),
        },
        Op::Withdraw(account, x) => Command::Withdraw {
            account,
            amount: Fx::from_int(x),
        },
        Op::Trade(account, market, q, k, off) => {
            let mark = e.mark(market).expect("genesis sets marks");
            Command::Trade {
                account,
                market,
                qty: Fx::from_raw(q * 10i64.pow(k)),
                price: mark + mark.bps(off),
            }
        }
        Op::Move(market, bps) => {
            let mark = e.mark(market).expect("genesis sets marks");
            let next = mark + mark.bps(bps);
            Command::Mark {
                market,
                price: if next.is_positive() { next } else { mark },
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2_048, ..ProptestConfig::default() })]

    #[test]
    fn engine_invariants_hold(ops in proptest::collection::vec(op(), 1..200)) {
        let mut a = Engine::new();
        let mut b = Engine::new();
        let mut out = Vec::new();
        let mut log = genesis();
        for c in &log {
            a.apply(c, &mut out);
            b.apply(c, &mut out);
        }
        for o in &ops {
            let cmd = resolve(&a, o);
            a.apply(&cmd, &mut out);
            b.apply(&cmd, &mut out);
            prop_assert_eq!(a.transcript_hash(), b.transcript_hash());
            if let Err(e) = a.check_invariants() {
                return Err(TestCaseError::fail(format!("{e} after {cmd:?}")));
            }
            if matches!(cmd, Command::Mark { .. })
                && let Err(e) = a.check_no_liquidatable()
            {
                return Err(TestCaseError::fail(format!("{e} after {cmd:?}")));
            }
            log.push(cmd);
        }

        // What the WAL stores must replay to the identical transcript.
        let json: Vec<String> = log.iter().map(|c| serde_json::to_string(c).unwrap()).collect();
        let mut replica = Engine::new();
        for line in &json {
            let cmd: Command = serde_json::from_str(line).unwrap();
            replica.apply(&cmd, &mut out);
        }
        prop_assert_eq!(replica.transcript_hash(), a.transcript_hash());
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 20_000, ..ProptestConfig::default() })]

    /// The i64 fast paths must agree bit-for-bit with the i128 reference,
    /// including at the boundary where the product stops fitting.
    #[test]
    fn fast_path_matches_wide_reference(
        a in -10_000_000_000_000i64..10_000_000_000_000,
        b in -100_000_000_000i64..100_000_000_000,
    ) {
        // |a * b| reaches 1e24: well past i64 (9.2e18), so both paths run,
        // while the rescaled result always fits.
        let (x, y) = (Fx::from_raw(a), Fx::from_raw(b));
        prop_assert_eq!(x.mul_trunc(y), x.mul_trunc_wide(y));
        prop_assert_eq!(x.mul_ceil(y), x.mul_ceil_wide(y));
    }
}
