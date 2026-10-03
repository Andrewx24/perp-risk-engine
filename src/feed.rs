//! Venue market-data ingestion.
//!
//! Each venue runs as its own Tokio task: connect, subscribe, parse, forward.
//! Connections die — venues restart, load balancers recycle sockets, a
//! network blip eats a frame. So every task owns a reconnect loop with
//! exponential backoff and jitter, and a read deadline: a socket that is
//! open but silent is treated as dead, because a feed that has quietly
//! stopped is indistinguishable from a market that has stopped moving.

use crate::fixed::Fx;
use crate::orderbook::{BookError, Level, OrderBook, Side};
use crate::types::{MarketId, now_ms};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// A normalized market-data message from any venue.
#[derive(Clone, Debug, PartialEq)]
pub enum FeedMsg {
    /// Best bid/offer only (venues that publish a BBO stream).
    Top { bid: Fx, ask: Fx },
    Snapshot {
        seq: u64,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    Delta {
        first_seq: u64,
        last_seq: u64,
        changes: Vec<(Side, Level)>,
    },
    /// The connection dropped; any book built from it is now unknown.
    Disconnected,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VenueUpdate {
    pub venue: &'static str,
    pub market: MarketId,
    pub ts_ms: u64,
    pub msg: FeedMsg,
}

/// Turns per-venue feed messages into mid prices, maintaining a sequenced
/// book for venues that publish depth.
#[derive(Default)]
pub struct BookTracker {
    books: HashMap<(&'static str, MarketId), OrderBook>,
    pub gaps: u64,
}

impl BookTracker {
    pub fn on_update(&mut self, u: &VenueUpdate) -> Option<Fx> {
        let key = (u.venue, u.market);
        match &u.msg {
            FeedMsg::Top { bid, ask } => {
                (bid.is_positive() && bid < ask).then(|| Fx::from_raw((bid.raw() + ask.raw()) / 2))
            }
            FeedMsg::Snapshot { seq, bids, asks } => {
                let book = self.books.entry(key).or_default();
                book.apply_snapshot(*seq, bids, asks).ok()?;
                book.mid()
            }
            FeedMsg::Delta {
                first_seq,
                last_seq,
                changes,
            } => {
                let book = self.books.entry(key).or_default();
                match book.apply_delta(*first_seq, *last_seq, changes) {
                    Ok(_) => book.mid(),
                    Err(BookError::NotSynced) => None,
                    Err(e) => {
                        self.gaps += 1;
                        tracing::warn!(venue = u.venue, market = %u.market, error = %e, "book desynced; waiting for snapshot");
                        None
                    }
                }
            }
            FeedMsg::Disconnected => {
                if let Some(b) = self.books.get_mut(&key) {
                    b.desync();
                }
                None
            }
        }
    }

    pub fn book(&self, venue: &'static str, market: MarketId) -> Option<&OrderBook> {
        self.books.get(&(venue, market))
    }
}

/// A venue's wire protocol: where to connect, what to send, how to parse.
pub trait VenueAdapter: Send + 'static {
    fn name(&self) -> &'static str;
    fn url(&self) -> String;
    fn subscriptions(&self) -> Vec<String>;
    /// Parses one text frame. Non-data frames (acks, pongs) return `None`.
    fn parse(&mut self, text: &str) -> Option<(MarketId, FeedMsg)>;
}

fn levels(raw: &[Vec<String>]) -> Option<Vec<Level>> {
    raw.iter()
        .map(|l| {
            Some(Level::new(
                l.first()?.parse().ok()?,
                l.get(1)?.parse().ok()?,
            ))
        })
        .collect()
}

/// Binance USD-M futures `<symbol>@bookTicker`.
#[derive(Clone)]
pub struct Binance {
    pub symbols: Vec<(String, MarketId)>,
}

#[derive(Deserialize)]
struct BinanceBookTicker {
    s: String,
    b: String,
    a: String,
}

impl VenueAdapter for Binance {
    fn name(&self) -> &'static str {
        "binance"
    }
    fn url(&self) -> String {
        let streams: Vec<String> = self
            .symbols
            .iter()
            .map(|(s, _)| format!("{}@bookTicker", s.to_lowercase()))
            .collect();
        format!("wss://fstream.binance.com/ws/{}", streams.join("/"))
    }
    fn subscriptions(&self) -> Vec<String> {
        Vec::new()
    }
    fn parse(&mut self, text: &str) -> Option<(MarketId, FeedMsg)> {
        let t: BinanceBookTicker = serde_json::from_str(text).ok()?;
        let market = self
            .symbols
            .iter()
            .find(|(s, _)| s.eq_ignore_ascii_case(&t.s))?
            .1;
        Some((
            market,
            FeedMsg::Top {
                bid: t.b.parse().ok()?,
                ask: t.a.parse().ok()?,
            },
        ))
    }
}

/// OKX v5 public `bbo-tbt` (tick-by-tick best bid/offer).
#[derive(Clone)]
pub struct Okx {
    pub inst_ids: Vec<(String, MarketId)>,
}

#[derive(Deserialize)]
struct OkxArg {
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Deserialize)]
struct OkxBbo {
    bids: Vec<Vec<String>>,
    asks: Vec<Vec<String>>,
}

#[derive(Deserialize)]
struct OkxPush {
    arg: OkxArg,
    data: Vec<OkxBbo>,
}

impl VenueAdapter for Okx {
    fn name(&self) -> &'static str {
        "okx"
    }
    fn url(&self) -> String {
        "wss://ws.okx.com:8443/ws/v5/public".into()
    }
    fn subscriptions(&self) -> Vec<String> {
        let args: Vec<_> = self
            .inst_ids
            .iter()
            .map(|(id, _)| serde_json::json!({ "channel": "bbo-tbt", "instId": id }))
            .collect();
        vec![serde_json::json!({ "op": "subscribe", "args": args }).to_string()]
    }
    fn parse(&mut self, text: &str) -> Option<(MarketId, FeedMsg)> {
        let p: OkxPush = serde_json::from_str(text).ok()?;
        let market = self.inst_ids.iter().find(|(s, _)| *s == p.arg.inst_id)?.1;
        let d = p.data.first()?;
        let bid = levels(&d.bids)?.first()?.price;
        let ask = levels(&d.asks)?.first()?.price;
        Some((market, FeedMsg::Top { bid, ask }))
    }
}

/// Bybit v5 linear `orderbook.1.<symbol>`: a sequenced snapshot + delta
/// stream, so it goes through the gap-checked [`OrderBook`].
#[derive(Clone)]
pub struct Bybit {
    pub symbols: Vec<(String, MarketId)>,
}

#[derive(Deserialize)]
struct BybitBook {
    s: String,
    b: Vec<Vec<String>>,
    a: Vec<Vec<String>>,
    u: u64,
}

#[derive(Deserialize)]
struct BybitPush {
    #[serde(rename = "type")]
    kind: String,
    data: BybitBook,
}

impl VenueAdapter for Bybit {
    fn name(&self) -> &'static str {
        "bybit"
    }
    fn url(&self) -> String {
        "wss://stream.bybit.com/v5/public/linear".into()
    }
    fn subscriptions(&self) -> Vec<String> {
        let args: Vec<String> = self
            .symbols
            .iter()
            .map(|(s, _)| format!("orderbook.1.{s}"))
            .collect();
        vec![serde_json::json!({ "op": "subscribe", "args": args }).to_string()]
    }
    fn parse(&mut self, text: &str) -> Option<(MarketId, FeedMsg)> {
        let p: BybitPush = serde_json::from_str(text).ok()?;
        let market = self.symbols.iter().find(|(s, _)| *s == p.data.s)?.1;
        let (bids, asks) = (levels(&p.data.b)?, levels(&p.data.a)?);
        // u == 1 means the venue restarted its book: treat as a snapshot.
        if p.kind == "snapshot" || p.data.u == 1 {
            return Some((
                market,
                FeedMsg::Snapshot {
                    seq: p.data.u,
                    bids,
                    asks,
                },
            ));
        }
        let changes = bids
            .into_iter()
            .map(|l| (Side::Bid, l))
            .chain(asks.into_iter().map(|l| (Side::Ask, l)))
            .collect();
        Some((
            market,
            FeedMsg::Delta {
                first_seq: p.data.u,
                last_seq: p.data.u,
                changes,
            },
        ))
    }
}

/// Runs a venue under supervision. A panic inside a feed task is a bug, but
/// it must not silently and permanently remove a price source from the
/// oracle: log it loudly and restart from a fresh adapter.
pub async fn supervise<A: VenueAdapter + Clone>(
    adapter: A,
    tx: mpsc::Sender<VenueUpdate>,
    markets: Vec<MarketId>,
) {
    let venue = adapter.name();
    loop {
        let task = tokio::spawn(run_venue(adapter.clone(), tx.clone(), markets.clone()));
        match task.await {
            Ok(()) => return,
            Err(e) => {
                tracing::error!(venue, error = %e, "feed task died; restarting");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Runs one venue forever, reconnecting with capped exponential backoff.
pub async fn run_venue<A: VenueAdapter>(
    mut adapter: A,
    tx: mpsc::Sender<VenueUpdate>,
    markets: Vec<MarketId>,
) {
    let venue = adapter.name();
    let mut backoff = Duration::from_millis(250);
    let max_backoff = Duration::from_secs(30);
    let read_deadline = Duration::from_secs(15);
    loop {
        let url = adapter.url();
        tracing::info!(venue, %url, "connecting");
        match tokio_tungstenite::connect_async(url.as_str()).await {
            Ok((mut ws, _)) => {
                let mut ok = true;
                for sub in adapter.subscriptions() {
                    if ws.send(Message::text(sub)).await.is_err() {
                        ok = false;
                    }
                }
                if ok {
                    tracing::info!(venue, "subscribed");
                }
                let mut received = false;
                loop {
                    if !ok {
                        break;
                    }
                    match tokio::time::timeout(read_deadline, ws.next()).await {
                        Err(_) => {
                            tracing::warn!(venue, "read deadline exceeded; reconnecting");
                            break;
                        }
                        Ok(None) | Ok(Some(Err(_))) => break,
                        Ok(Some(Ok(Message::Text(t)))) => {
                            if let Some((market, msg)) = adapter.parse(&t) {
                                received = true;
                                let u = VenueUpdate {
                                    venue,
                                    market,
                                    ts_ms: now_ms(),
                                    msg,
                                };
                                if tx.send(u).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Ok(Some(Ok(Message::Ping(p)))) => {
                            let _ = ws.send(Message::Pong(p)).await;
                        }
                        Ok(Some(Ok(Message::Close(_)))) => break,
                        Ok(Some(Ok(_))) => {}
                    }
                }
                if received {
                    backoff = Duration::from_millis(250);
                }
            }
            Err(e) => tracing::warn!(venue, error = %e, "connect failed"),
        }
        for &market in &markets {
            let u = VenueUpdate {
                venue,
                market,
                ts_ms: now_ms(),
                msg: FeedMsg::Disconnected,
            };
            if tx.send(u).await.is_err() {
                return;
            }
        }
        // Full jitter: spread reconnects so a venue-wide outage does not end
        // in every client reconnecting in the same millisecond.
        let jitter = (now_ms() % 1000) as f64 / 1000.0;
        tokio::time::sleep(backoff.mul_f64(0.5 + jitter / 2.0)).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BTC: MarketId = MarketId(1);

    fn fx(s: &str) -> Fx {
        s.parse().unwrap()
    }

    #[test]
    fn parses_binance_book_ticker() {
        let mut a = Binance {
            symbols: vec![("BTCUSDT".into(), BTC)],
        };
        let msg = r#"{"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BTCUSDT","b":"65000.10","B":"31.21","a":"65000.20","A":"40.66"}"#;
        assert_eq!(
            a.parse(msg),
            Some((
                BTC,
                FeedMsg::Top {
                    bid: fx("65000.1"),
                    ask: fx("65000.2")
                }
            ))
        );
        assert!(a.url().ends_with("/btcusdt@bookTicker"));
    }

    #[test]
    fn parses_okx_bbo_and_ignores_acks() {
        let mut a = Okx {
            inst_ids: vec![("BTC-USDT-SWAP".into(), BTC)],
        };
        let ack = r#"{"event":"subscribe","arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"connId":"a4d3ae55"}"#;
        assert_eq!(a.parse(ack), None);
        let msg = r#"{"arg":{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["65001.5","415","0","13"]],"bids":[["65001.4","256","0","12"]],"ts":"1597026383085","seqId":123}]}"#;
        assert_eq!(
            a.parse(msg),
            Some((
                BTC,
                FeedMsg::Top {
                    bid: fx("65001.4"),
                    ask: fx("65001.5")
                }
            ))
        );
    }

    #[test]
    fn parses_bybit_snapshot_and_delta() {
        let mut a = Bybit {
            symbols: vec![("BTCUSDT".into(), BTC)],
        };
        let snap = r#"{"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1672304484978,"data":{"s":"BTCUSDT","b":[["16493.50","0.006"]],"a":[["16611.00","0.029"]],"u":18521288,"seq":7961638724},"cts":1672304484976}"#;
        let delta = r#"{"topic":"orderbook.1.BTCUSDT","type":"delta","ts":1672304484979,"data":{"s":"BTCUSDT","b":[["16493.50","0"],["16494.00","1"]],"a":[],"u":18521289,"seq":7961638725},"cts":1672304484977}"#;
        let mut t = BookTracker::default();
        let (m, msg) = a.parse(snap).unwrap();
        let u = VenueUpdate {
            venue: "bybit",
            market: m,
            ts_ms: 0,
            msg,
        };
        assert_eq!(t.on_update(&u), Some(fx("16552.25")));
        let (m, msg) = a.parse(delta).unwrap();
        let u = VenueUpdate {
            venue: "bybit",
            market: m,
            ts_ms: 0,
            msg,
        };
        assert_eq!(t.on_update(&u), Some(fx("16552.5")));
    }

    #[test]
    fn tracker_drops_quotes_after_a_gap_until_snapshot() {
        let mut t = BookTracker::default();
        let up = |msg| VenueUpdate {
            venue: "sim",
            market: BTC,
            ts_ms: 0,
            msg,
        };
        let lvl = |p: &str| vec![Level::new(fx(p), fx("1"))];
        assert!(
            t.on_update(&up(FeedMsg::Snapshot {
                seq: 1,
                bids: lvl("99"),
                asks: lvl("101")
            }))
            .is_some()
        );
        assert_eq!(
            t.on_update(&up(FeedMsg::Delta {
                first_seq: 3,
                last_seq: 3,
                changes: vec![]
            })),
            None
        );
        assert_eq!(t.gaps, 1);
        assert_eq!(
            t.on_update(&up(FeedMsg::Delta {
                first_seq: 4,
                last_seq: 4,
                changes: vec![]
            })),
            None
        );
        assert!(
            t.on_update(&up(FeedMsg::Snapshot {
                seq: 9,
                bids: lvl("99"),
                asks: lvl("101")
            }))
            .is_some()
        );
    }
}
