//! Runtime wiring: the engine thread and the oracle task.
//!
//! ```text
//! venue tasks ──VenueUpdate──▶ oracle task ──Command::Mark──┐
//!  (Tokio, one per venue)       (books + quorum)              ▼
//!                                              ┌──────── engine thread ────────┐
//! HTTP / WS handlers ──Request (bounded mpsc)─▶│ drain batch → WAL append →    │
//!                                              │ fsync once → apply → reply    │
//!                                              └──┬──────────────┬─────────────┘
//!                                   watch<Arc<Snapshot>>   broadcast<Arc<str>>
//!                                      (reads, no lock)     (pre-serialized events)
//! ```
//!
//! The engine is single-writer and lock-free by construction: it lives on a
//! dedicated OS thread and owns all state. Tokio does I/O; it never runs the
//! engine, so a slow socket can never stall a liquidation and a burst of
//! liquidations can never starve the executor that is reading the feeds.

use crate::engine::{AccountView, Command, Engine, EngineStats, Event};
use crate::feed::{BookTracker, VenueUpdate};
use crate::fixed::Fx;
use crate::latency::{Histogram, Summary};
use crate::margin::MarketConfig;
use crate::oracle::{Consensus, Observation, OracleConfig, OracleError, PriceOracle};
use crate::types::{AccountId, MarketId, now_ms};
use crate::wal::Wal;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, oneshot, watch};

pub enum Request {
    Command {
        cmd: Command,
        reply: Option<oneshot::Sender<Vec<Event>>>,
    },
    Account {
        id: AccountId,
        reply: oneshot::Sender<Option<AccountView>>,
    },
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct EngineSnapshot {
    pub seq: u64,
    pub transcript_hash: String,
    pub insurance_fund: Fx,
    pub accounts: usize,
    pub stats: EngineStats,
    pub marks: BTreeMap<MarketId, Fx>,
    pub markets: Vec<MarketConfig>,
    /// Time to apply one command, measured on the engine thread.
    pub apply_latency: Summary,
    /// Time from a request entering the queue to its batch being applied.
    pub queue_to_apply_latency: Summary,
    pub wal_sync_latency: Summary,
    pub batches: u64,
}

#[derive(Clone)]
pub struct EngineHandle {
    pub tx: mpsc::Sender<(Instant, Request)>,
    pub snapshot: watch::Receiver<Arc<EngineSnapshot>>,
    pub events: broadcast::Sender<Arc<str>>,
}

impl EngineHandle {
    pub async fn submit(&self, cmd: Command) -> Option<Vec<Event>> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send((
                Instant::now(),
                Request::Command {
                    cmd,
                    reply: Some(reply),
                },
            ))
            .await
            .ok()?;
        rx.await.ok()
    }

    /// Fire-and-forget: used by the oracle, which does not wait for events.
    pub async fn send(&self, cmd: Command) -> bool {
        self.tx
            .send((Instant::now(), Request::Command { cmd, reply: None }))
            .await
            .is_ok()
    }

    pub async fn account(&self, id: AccountId) -> Option<AccountView> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send((Instant::now(), Request::Account { id, reply }))
            .await
            .ok()?;
        rx.await.ok().flatten()
    }
}

const MAX_BATCH: usize = 1024;

pub fn spawn_engine(mut engine: Engine, mut wal: Option<Wal>, queue: usize) -> EngineHandle {
    let (tx, mut rx) = mpsc::channel::<(Instant, Request)>(queue);
    let (snap_tx, snapshot) = watch::channel(Arc::new(snapshot_of(&engine, &Default::default())));
    let (events, _) = broadcast::channel::<Arc<str>>(4096);
    let ev_tx = events.clone();

    std::thread::Builder::new()
        .name("engine".into())
        .spawn(move || {
            let mut lat = Latencies::default();
            let mut batch = Vec::with_capacity(MAX_BATCH);
            let mut out = Vec::new();
            let mut per_cmd = Vec::new();
            while let Some(first) = rx.blocking_recv() {
                batch.clear();
                batch.push(first);
                while batch.len() < MAX_BATCH {
                    match rx.try_recv() {
                        Ok(r) => batch.push(r),
                        Err(_) => break,
                    }
                }

                // Group commit: every command in the batch is durable before
                // any of them is applied or acknowledged.
                if let Some(w) = wal.as_mut() {
                    let t = Instant::now();
                    for (_, r) in &batch {
                        if let Request::Command { cmd, .. } = r {
                            w.append(cmd)
                                .expect("WAL append failed; refusing to ack undurable commands");
                        }
                    }
                    w.sync()
                        .expect("WAL sync failed; refusing to ack undurable commands");
                    lat.wal.record(t.elapsed().as_nanos() as u64);
                }

                out.clear();
                for (enqueued, r) in batch.drain(..) {
                    match r {
                        Request::Command { cmd, reply } => {
                            let t = Instant::now();
                            per_cmd.clear();
                            engine.apply(&cmd, &mut per_cmd);
                            lat.apply.record(t.elapsed().as_nanos() as u64);
                            lat.queue.record(enqueued.elapsed().as_nanos() as u64);
                            out.extend_from_slice(&per_cmd);
                            if let Some(reply) = reply {
                                let _ = reply.send(per_cmd.clone());
                            }
                        }
                        Request::Account { id, reply } => {
                            let _ = reply.send(engine.account_view(id));
                        }
                    }
                }
                lat.batches += 1;

                // Serialize once per batch, however many clients are listening.
                if !out.is_empty()
                    && ev_tx.receiver_count() > 0
                    && let Ok(json) = serde_json::to_string(&out)
                {
                    let _ = ev_tx.send(json.into());
                }
                snap_tx.send_replace(Arc::new(snapshot_of(&engine, &lat)));
            }
            tracing::info!(
                seq = engine.seq(),
                hash = format!("{:016x}", engine.transcript_hash()),
                "engine stopped"
            );
        })
        .expect("spawn engine thread");

    EngineHandle {
        tx,
        snapshot,
        events,
    }
}

#[derive(Default)]
struct Latencies {
    apply: Histogram,
    queue: Histogram,
    wal: Histogram,
    batches: u64,
}

fn snapshot_of(e: &Engine, lat: &Latencies) -> EngineSnapshot {
    EngineSnapshot {
        seq: e.seq(),
        transcript_hash: format!("{:016x}", e.transcript_hash()),
        insurance_fund: e.insurance_fund(),
        accounts: e.account_count(),
        stats: e.stats(),
        marks: e.marks().clone(),
        markets: e.markets().cloned().collect(),
        apply_latency: lat.apply.summary(),
        queue_to_apply_latency: lat.queue.summary(),
        wal_sync_latency: lat.wal.summary(),
        batches: lat.batches,
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct OracleStatus {
    pub consensus: Option<Consensus>,
    pub error: Option<OracleError>,
    pub venue_mids: BTreeMap<&'static str, Fx>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct OracleView {
    pub markets: BTreeMap<MarketId, OracleStatus>,
    pub book_gaps: u64,
    pub marks_sent: u64,
}

/// Consumes venue updates, maintains books, votes, and forwards consensus
/// marks to the engine. Marks are throttled: at most one per market per
/// `min_interval_ms` unless the price is unchanged, then a heartbeat.
pub async fn run_oracle(
    mut rx: mpsc::Receiver<VenueUpdate>,
    engine: EngineHandle,
    cfg: OracleConfig,
    view_tx: watch::Sender<Arc<OracleView>>,
) {
    let min_interval_ms = 10;
    let heartbeat_ms = 1_000;
    let mut tracker = BookTracker::default();
    let mut oracles: BTreeMap<MarketId, PriceOracle> = BTreeMap::new();
    let mut last_sent: BTreeMap<MarketId, (Fx, u64)> = BTreeMap::new();
    let mut view = OracleView::default();
    let mut last_publish = 0;

    while let Some(u) = rx.recv().await {
        let now = now_ms();
        let status = view.markets.entry(u.market).or_default();
        match tracker.on_update(&u) {
            Some(mid) => {
                status.venue_mids.insert(u.venue, mid);
                oracles
                    .entry(u.market)
                    .or_insert_with(|| PriceOracle::new(cfg))
                    .submit(Observation {
                        source: u.venue,
                        price: mid,
                        ts_ms: u.ts_ms,
                    });
            }
            None => {
                status.venue_mids.remove(u.venue);
            }
        }
        let Some(oracle) = oracles.get(&u.market) else {
            continue;
        };
        match oracle.aggregate(now) {
            Ok(c) => {
                let price = c.price;
                status.consensus = Some(c);
                status.error = None;
                let due = match last_sent.get(&u.market) {
                    None => true,
                    Some(&(p, t)) => {
                        (p != price && now >= t + min_interval_ms) || now >= t + heartbeat_ms
                    }
                };
                if due {
                    last_sent.insert(u.market, (price, now));
                    view.marks_sent += 1;
                    if !engine
                        .send(Command::Mark {
                            market: u.market,
                            price,
                        })
                        .await
                    {
                        return;
                    }
                }
            }
            Err(e) => {
                if status.error.as_ref() != Some(&e) {
                    tracing::warn!(market = %u.market, error = %e, "oracle withholding mark");
                }
                status.error = Some(e);
            }
        }
        view.book_gaps = tracker.gaps;
        if now >= last_publish + 50 {
            last_publish = now;
            view_tx.send_replace(Arc::new(view.clone()));
        }
    }
}
