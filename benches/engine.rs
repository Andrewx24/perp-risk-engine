use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use perp_risk_engine::oracle::{Observation, OracleConfig, PriceOracle};
use perp_risk_engine::orderbook::{Level, OrderBook, Side};
use perp_risk_engine::{AccountId, Command, Engine, Fx, MarketId, default_markets};
use std::hint::black_box;

const BTC: MarketId = MarketId(1);
const ETH: MarketId = MarketId(2);

/// An engine with `n` accounts, each holding a modest BTC position, and, if
/// `cross`, a second position in ETH that takes it off the trigger index.
fn engine_with(n: u64, cross: bool) -> Engine {
    let mut e = Engine::new();
    let mut out = Vec::new();
    for m in default_markets() {
        e.apply(&Command::CreateMarket(m), &mut out);
    }
    e.apply(
        &Command::Mark {
            market: BTC,
            price: Fx::from_int(65_000),
        },
        &mut out,
    );
    e.apply(
        &Command::Mark {
            market: ETH,
            price: Fx::from_int(3_200),
        },
        &mut out,
    );
    for i in 1..=n {
        let account = AccountId(i);
        e.apply(
            &Command::Deposit {
                account,
                amount: Fx::from_int(10_000),
            },
            &mut out,
        );
        let qty = Fx::from_raw(if i % 2 == 0 { 300_000 } else { -300_000 });
        e.apply(
            &Command::Trade {
                account,
                market: BTC,
                qty,
                price: Fx::from_int(65_000),
            },
            &mut out,
        );
        if cross {
            e.apply(
                &Command::Trade {
                    account,
                    market: ETH,
                    qty: Fx::ONE,
                    price: Fx::from_int(3_200),
                },
                &mut out,
            );
        }
    }
    e
}

fn mark_update(c: &mut Criterion) {
    bench_marks(c, "mark_update_single_market_accounts", false);
    bench_marks(c, "mark_update_cross_margined_accounts", true);
}

fn bench_marks(c: &mut Criterion, name: &str, cross: bool) {
    let mut g = c.benchmark_group(name);
    for n in [1_000u64, 10_000, 100_000] {
        let mut e = engine_with(n, cross);
        let mut out = Vec::with_capacity(16);
        let mut up = false;
        g.throughput(Throughput::Elements(n));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                // Alternate ±1 so state stays bounded and nobody liquidates.
                up = !up;
                let price = Fx::from_int(if up { 65_001 } else { 64_999 });
                out.clear();
                e.apply(black_box(&Command::Mark { market: BTC, price }), &mut out);
            })
        });
    }
    g.finish();
}

fn trade(c: &mut Criterion) {
    let mut e = engine_with(10_000, false);
    let mut out = Vec::with_capacity(16);
    let mut i = 0u64;
    c.bench_function("trade_pre_trade_check_and_book", |b| {
        b.iter(|| {
            i += 1;
            // Buy then sell back: the position oscillates, the check always runs.
            let qty = Fx::from_raw(if i % 2 == 0 { 10_000 } else { -10_000 });
            out.clear();
            let cmd = Command::Trade {
                account: AccountId(1 + i % 10_000),
                market: BTC,
                qty,
                price: Fx::from_int(65_000),
            };
            e.apply(black_box(&cmd), &mut out);
        })
    });
}

fn book_delta(c: &mut Criterion) {
    let mut book = OrderBook::new();
    let bids: Vec<Level> = (0..50)
        .map(|i| Level::new(Fx::from_int(64_999 - i), Fx::ONE))
        .collect();
    let asks: Vec<Level> = (0..50)
        .map(|i| Level::new(Fx::from_int(65_001 + i), Fx::ONE))
        .collect();
    book.apply_snapshot(1, &bids, &asks).unwrap();
    let mut seq = 1u64;
    c.bench_function("orderbook_delta_4_levels_plus_mid", |b| {
        b.iter(|| {
            seq += 1;
            let q = Fx::from_raw(1_000_000 + (seq % 7) as i64 * 100_000);
            let changes = [
                (Side::Bid, Level::new(Fx::from_int(64_999), q)),
                (Side::Bid, Level::new(Fx::from_int(64_990), q)),
                (Side::Ask, Level::new(Fx::from_int(65_001), q)),
                (Side::Ask, Level::new(Fx::from_int(65_010), q)),
            ];
            book.apply_delta(seq, seq, black_box(&changes)).unwrap();
            black_box(book.mid())
        })
    });
}

fn oracle(c: &mut Criterion) {
    let mut o = PriceOracle::new(OracleConfig {
        min_sources: 3,
        max_staleness_ms: 1_000,
        max_deviation_bps: 100,
    });
    for (i, s) in ["a", "b", "c", "d", "e"].into_iter().enumerate() {
        o.submit(Observation {
            source: s,
            price: Fx::from_int(65_000 + i as i64),
            ts_ms: 1_000,
        });
    }
    c.bench_function("oracle_quorum_5_sources", |b| {
        b.iter(|| black_box(o.aggregate(black_box(1_500))))
    });
}

fn parse(c: &mut Criterion) {
    c.bench_function("fx_parse_venue_price", |b| {
        b.iter(|| black_box("65001.25000000").parse::<Fx>())
    });
}

criterion_group!(benches, mark_update, trade, book_delta, oracle, parse);
criterion_main!(benches);
