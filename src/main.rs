use perp_risk_engine::engine::{Command, Engine};
use perp_risk_engine::feed::{self, Binance, Bybit, Okx};
use perp_risk_engine::oracle::OracleConfig;
use perp_risk_engine::server::{AppState, router};
use perp_risk_engine::service::{OracleView, run_oracle, spawn_engine};
use perp_risk_engine::sim::{MarketSim, SimMarket, TraderSim};
use perp_risk_engine::{Fx, MarketId, default_markets};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

const USAGE: &str = "\
perp-risk-engine [--mode sim|live] [--addr HOST:PORT] [--wal PATH]
                 [--seed N] [--traders N] [--rate CMDS_PER_SEC]

  --mode     sim: seeded synthetic venues (default)
             live: Binance, OKX and Bybit public WebSocket feeds
  --addr     HTTP/WebSocket listen address (default 127.0.0.1:8080)
  --wal      command log; replayed on startup, appended while running
  --seed     simulation seed (default 1)
  --traders  simulated accounts trading against the marks (default 2000, 0 = none)
  --rate     simulated trader commands per second (default 2000)";

struct Args {
    live: bool,
    addr: String,
    wal: Option<PathBuf>,
    seed: u64,
    traders: u64,
    rate: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        live: false,
        addr: "127.0.0.1:8080".into(),
        wal: None,
        seed: 1,
        traders: 2_000,
        rate: 2_000,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut val = || it.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--mode" => {
                a.live = match val()?.as_str() {
                    "live" => true,
                    "sim" => false,
                    m => return Err(format!("unknown mode {m}")),
                }
            }
            "--addr" => a.addr = val()?,
            "--wal" => a.wal = Some(val()?.into()),
            "--seed" => a.seed = val()?.parse().map_err(|e| format!("--seed: {e}"))?,
            "--traders" => a.traders = val()?.parse().map_err(|e| format!("--traders: {e}"))?,
            "--rate" => a.rate = val()?.parse().map_err(|e| format!("--rate: {e}"))?,
            "-h" | "--help" => return Err(String::new()),
            f => return Err(format!("unknown flag {f}")),
        }
    }
    Ok(a)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // tokio-tungstenite's rustls has no crypto backend compiled in by default;
    // without this every TLS handshake panics.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            if !e.is_empty() {
                eprintln!("error: {e}\n");
            }
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };

    let mut engine = Engine::new();
    let wal = match &args.wal {
        Some(path) => {
            let log = perp_risk_engine::wal::Wal::read_all(path)?;
            let mut scratch = Vec::new();
            for cmd in &log {
                scratch.clear();
                engine.apply(cmd, &mut scratch);
            }
            tracing::info!(
                commands = log.len(),
                hash = format!("{:016x}", engine.transcript_hash()),
                "replayed WAL"
            );
            Some(perp_risk_engine::wal::Wal::open(path)?)
        }
        None => None,
    };
    let fresh = engine.markets().next().is_none();
    let handle = spawn_engine(engine, wal, 65_536);
    if fresh {
        for m in default_markets() {
            handle.submit(Command::CreateMarket(m)).await;
        }
    }

    let (feed_tx, feed_rx) = mpsc::channel(65_536);
    let oracle_cfg = OracleConfig {
        min_sources: 2,
        max_staleness_ms: 2_000,
        max_deviation_bps: 100,
    };
    let (oracle_tx, oracle_rx) = watch::channel(Arc::new(OracleView::default()));
    tokio::spawn(run_oracle(feed_rx, handle.clone(), oracle_cfg, oracle_tx));

    let (btc, eth, sol) = (MarketId(1), MarketId(2), MarketId(3));
    if args.live {
        let all = vec![btc, eth, sol];
        let pairs = |f: fn(&str) -> String| vec![(f("BTC"), btc), (f("ETH"), eth), (f("SOL"), sol)];
        tokio::spawn(feed::supervise(
            Binance {
                symbols: pairs(|c| format!("{c}USDT")),
            },
            feed_tx.clone(),
            all.clone(),
        ));
        tokio::spawn(feed::supervise(
            Okx {
                inst_ids: pairs(|c| format!("{c}-USDT-SWAP")),
            },
            feed_tx.clone(),
            all.clone(),
        ));
        tokio::spawn(feed::supervise(
            Bybit {
                symbols: pairs(|c| format!("{c}USDT")),
            },
            feed_tx.clone(),
            all,
        ));
    } else {
        let seed = args.seed;
        let tx = feed_tx.clone();
        tokio::spawn(async move {
            let mut sim = MarketSim::new(
                seed,
                vec![
                    SimMarket {
                        market: btc,
                        price: 65_000.0,
                        tick: Fx::from_raw(500_000),
                        sigma: 0.0002,
                        jump_prob: 0.0002,
                    },
                    SimMarket {
                        market: eth,
                        price: 3_200.0,
                        tick: Fx::from_raw(10_000),
                        sigma: 0.0003,
                        jump_prob: 0.0002,
                    },
                    SimMarket {
                        market: sol,
                        price: 150.0,
                        tick: Fx::from_raw(1_000),
                        sigma: 0.0004,
                        jump_prob: 0.0003,
                    },
                ],
            );
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            let mut buf = Vec::new();
            loop {
                tick.tick().await;
                buf.clear();
                sim.step(&mut buf);
                for u in buf.drain(..) {
                    if tx.send(u).await.is_err() {
                        return;
                    }
                }
            }
        });
    }

    if args.traders > 0 && args.rate > 0 {
        let h = handle.clone();
        let (traders, rate, seed) = (args.traders, args.rate, args.seed);
        tokio::spawn(async move {
            let mut sim = TraderSim::new(seed, traders);
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            let per_tick = (rate / 100).max(1);
            loop {
                tick.tick().await;
                let marks = h.snapshot.borrow().marks.clone();
                if marks.is_empty() {
                    continue;
                }
                for _ in 0..per_tick {
                    if let Some(cmd) = sim.next_command(&marks)
                        && !h.send(cmd).await
                    {
                        return;
                    }
                }
            }
        });
    }

    let app = router(AppState {
        engine: handle,
        oracle: oracle_rx,
    });
    let listener = tokio::net::TcpListener::bind(&args.addr).await?;
    tracing::info!(addr = %args.addr, mode = if args.live { "live" } else { "sim" }, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
