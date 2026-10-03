# perp-risk-engine

A deterministic risk and liquidation engine for perpetual futures, written in Rust. It's fed by a
quorum price oracle over venue WebSocket feeds and served over REST and WebSocket.

```
venues (WS) ─▶ sequenced L2 books ─▶ quorum oracle ─▶ ┌──────────── engine thread ────────────┐
 Binance                gap → desync    median of       │ batch → WAL → fsync → apply → reply   │
 OKX                    until snapshot  agreeing,       │ margin checks · liquidation ·         │
 Bybit / sim                            fresh sources   │ insurance fund · loss socialization   │
                                                        └───────┬───────────────────┬───────────┘
 REST / WS clients ─────── bounded queue (503 when full) ──▶    watch<Snapshot>    broadcast<events>
```

The engine is a pure function `(state, command) → (state', events)`. It uses no clock, no
randomness, no floats, and no hash-map iteration order. Every transition extends a 64-bit
**transcript hash**, so two replicas, or a process and its own write-ahead log, can show they
agree by comparing one integer per sequence number.

## What's in it

| Module | What it does |
| --- | --- |
| [`fixed`](src/fixed.rs) | `i64` fixed-point at 10⁻⁶. Products go through `i128`, rounding direction is explicit (requirements round *up*), parsing is exact (`"0.0000001"` is an error, not a silent round), and overflow panics in release too. |
| [`orderbook`](src/orderbook.rs) | L2 book from snapshot + deltas. A sequence gap or a crossed book clears the book, and it won't quote again until a fresh snapshot arrives. |
| [`oracle`](src/oracle.rs) | Mark price = median of the venues that are fresh **and** agree within a band. Without a quorum, nothing is published. With 2f+1 sources, f liars can't move the mark. |
| [`margin`](src/margin.rs) | Positions with average-entry/realize/flip semantics. Cross margin: one equity pool, initial and maintenance requirements. |
| [`engine`](src/engine.rs) | The state machine: pre-trade checks, price bands, withdrawals capped at free collateral, partial liquidation, backstop vault, insurance fund, socialized losses, and the liquidation trigger index. |
| [`wal`](src/wal.rs) | Append-only command log with group commit. A torn final line is a crash and gets dropped; a bad line anywhere else is corruption and fails the load. |
| [`feed`](src/feed.rs) | Binance `bookTicker`, OKX `bbo-tbt`, Bybit `orderbook.1` adapters. Reconnects with jittered exponential backoff, uses a read deadline (a silent socket counts as dead), and runs under a supervisor that restarts a panicked feed. |
| [`service`](src/service.rs) | Runs the single-writer engine on a dedicated OS thread and the oracle as a Tokio task. |
| [`server`](src/server.rs) | Axum REST + WebSocket. Reads come from a `watch` snapshot and never touch the engine thread. Events are serialized once per batch, and slow WS clients get a `lagged` notice instead of buffering forever. |
| [`sim`](src/sim.rs) | Seeded venues that drop packets, go stale, print outliers and gap down, plus seeded traders. |
| [`latency`](src/latency.rs) | Allocation-free log-linear histogram (≤12.5% error, 496 buckets). Every command is timed on the engine thread. |

## Liquidation

When a mark update pushes an account under maintenance, the liquidation happens **in the same
transition**. The engine never has a window where it has accepted a price but not yet acted on it.

1. **Partial, largest risk first.** The engine closes 25% chunks of the position contributing the
   most maintenance margin until the account is healthy again. A dust remainder (< $10) or an
   account that's already bankrupt gets closed in one step.
2. **Backstop vault.** Closed size moves to a protocol vault at mark, the way Hyperliquid's HLP
   works, so a liquidation never depends on venue liquidity existing at the worst moment.
3. **Penalty → insurance fund.** The penalty is capped at the account's remaining equity, so the
   fund never ends up paying itself.
4. **Bad debt.** A flat account with negative collateral is covered by the insurance fund first.
   Any remainder is socialized pro rata across solvent accounts, rounded so it sums exactly, with
   anything that can't be absorbed recorded as a vault deficit and never dropped.
5. **Cascades.** A haircut can push *another* account under maintenance in a market the triggering
   mark never touched. Those accounts are re-checked before the transition ends. (The property
   tests found this case; see below.)

`MarketConfig::validate` rejects any market whose liquidation fee is ≥ its maintenance rate. In
that configuration closing a position makes the account *less* healthy, and the loop never
converges.

## Performance

The hot path is a mark update, which has to find every account the new price pushes under
maintenance. The obvious version re-margins every account exposed to that market, so it's O(n).

**Step 1: arithmetic.** Each `Fx` multiply divided through `i128`, and `i128 / constant` compiles to a
`__divti3` libcall. Every product that fits in an `i64` (all realistic price × size values) now
takes a 64-bit path where division by a constant becomes a multiply and a shift. A property test
checks the result is bit-identical to the `i128` reference across the overflow boundary. The trade
path got 52% faster. Mark updates improved only about 10%, which showed the remaining cost was the
O(n) walk, not the math.

**Step 2: stop visiting accounts that can't be liquidated.**

- *One position:* the liquidation price has a closed form. Long: `p < (e − C/s)/(1 − r)`. Short:
  `p > (C/s + e)/(1 + r)`. Accounts sit in a per-market `BTreeSet` keyed by that price, padded
  outward to cover rounding (`10 bps + 8µ$/size`), so rounding can only produce a false positive.
  A mark update is then a range query.
- *Several positions (cross margin):* there's no fixed liquidation price, because each market's
  boundary moves with the others. But `f = equity − maintenance` is separable, and a move Δp in
  market m changes it by at most `|s_m|(1 + r_m)|Δp|`. Each account gets a relative tolerance
  `k = (f − ε)/(notional + MM)` and a band `p_m(1 ± k)` in every market it holds. Inside all of its
  bands, `f > 0` is guaranteed. Leaving any band triggers an exact check, which then re-centers
  the bands.

The exact check stays the source of truth. The index only decides which accounts to check.

### Numbers

Measured on a shared 4-vCPU Xeon @ 2.1 GHz cloud container, release + fat LTO, `overflow-checks` on.

| | Before | After |
| --- | ---: | ---: |
| **Full pipeline, 2k sim traders, 3 markets: engine apply p99** | 458 µs | **15 µs** |
| Full pipeline: engine apply p99.9 | 655 µs | 41 µs |
| Mark update, 100k single-market accounts (±1 tick) | 17.3 ms | 77 ns |
| Mark update, 100k cross-margined accounts (±1 tick) | 18.7 ms | 73 ns |
| Pre-trade check + book a fill | 253 ns | ~300 ns |
| Order book delta (4 levels) + mid | | 46 ns |
| Oracle quorum over 5 sources | | 139 ns |
| Parse a venue price string exactly | | 23 ns |

Some caveats:

- The microbenchmarks move the mark one tick, so they show the *best* case: almost nobody crosses
  a trigger. Cost scales with k, the number of accounts the move actually touches. In a crash, k is
  large by definition, and those accounts need liquidating anyway. The pipeline row is the honest
  number.
- The trade path is ~50 ns slower than after step 1, because it now maintains the index. That's a
  deliberate trade: trades are per-account, but marks touch everyone.
- `queue → apply` latency (p50 ~160 µs, p99 ~0.5 ms without WAL) is dominated by batching and
  how the simulator bursts its load, not by the engine. With `--wal`, `fsync` group commit adds
  ~0.7 ms p50 on this container's disk.

Reproduce with `cargo bench`, or watch the live histograms at `GET /v1/state`.

## Testing

```bash
cargo test --release                                     # 43 tests
PROPTEST_CASES=100000 cargo test --release --test properties
```

- **Property tests** ([`tests/properties.rs`](tests/properties.rs)) generate arbitrary sequences of
  deposits, withdrawals, trades (from 0.000001 to 50 units, at prices around mark) and mark moves of
  up to ±30% per step. After **every** command they check structural invariants and trigger-index
  soundness. After every mark update they check, by full scan, that no account is left under
  maintenance. They also check that two engines agree on the transcript hash at every step, and
  that the JSON command log replays to the same hash.
  - These tests found a real bug the hand-written tests missed: socialized losses breaking
    accounts in other markets. The regression test is
    `socialized_loss_that_breaks_another_market_is_liquidated_in_the_same_step`.
  - To check the tests have teeth, the index was broken on purpose three ways: dropped `(1 − r)` from the
    threshold, padded inward instead of outward, and made bands 3× too wide. All three mutants were
    caught immediately.
- **Pipeline test** ([`tests/pipeline.rs`](tests/pipeline.rs)) runs simulated venues → books →
  oracle → engine with traders for 3,000 steps on logical time, asserts liquidations happen and
  invariants hold, and checks that the same seed gives the same transcript.
- **Crash recovery:** `kill -9` mid-run, then two independent restarts from copies of the torn WAL
  replay to the identical transcript hash.

## Running

```bash
cargo run --release                        # sim venues + 2,000 sim traders on :8080
cargo run --release -- --wal engine.wal    # durable; restart replays the log
cargo run --release -- --mode live --traders 0   # real Binance/OKX/Bybit feeds
```

```bash
curl localhost:8080/v1/state       # seq, transcript hash, marks, insurance fund, latency histograms
curl localhost:8080/v1/oracle      # per-venue mids, consensus, outliers, stale sources, book gaps
curl localhost:8080/v1/accounts/0  # the backstop vault's inventory
curl -X POST localhost:8080/v1/commands -H 'content-type: application/json' \
     -d '{"type":"deposit","account":42,"amount":"10000"}'
curl -X POST localhost:8080/v1/commands -H 'content-type: application/json' \
     -d '{"type":"trade","account":42,"market":1,"qty":"0.5","price":"65000"}'
websocat ws://localhost:8080/v1/stream   # every event, batched
```

`mark` and `create_market` are refused from clients with `403`. Prices come from the oracle, not
from whoever can reach the API.

## What's not here (yet)

- **Live adapters are unverified end to end.** They parse the venues' documented message formats
  and are unit-tested against those samples. The environment this was built in blocks exchange
  hosts, so they haven't been run against production endpoints. Running in `--mode live` is how
  you'd verify them (that run is also what caught the missing rustls crypto provider).
- **Replication.** The transcript hash is there so followers can verify a leader. The consensus
  layer that orders the log (Raft, or a chain's own block ordering) is out of scope.
- **Funding payments, fees, and ADL ranking** beyond pro-rata socialization.
- **Vault unwinding.** The backstop accumulates inventory but doesn't hedge it.
- **WAL compaction.** The log is JSON lines for greppability. A binary format plus periodic state
  snapshots would bound replay time.

## License

MIT
