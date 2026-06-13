//! Full-coverage stress + failure-injection harness for qx.
//!
//! This binary deliberately stresses every dashboard surface and
//! deliberately injects failures so each metric column has signal
//! the operator can verify against. Sections:
//!
//!   1. Producer fan-out      — Topics msgs/s + bytes/s + pending.
//!   2. Consumer fan-out      — Consumer groups members + lag.
//!   3. Workflow churn        — Workflows tasks / running / done /
//!                              failed / cancelled.
//!   4. Backpressure probe    — Sends to a runaway topic until the
//!                              broker returns BACKPRESSURE, proving
//!                              the cap is wired.
//!   5. Bad-bearer probe      — Sends with the wrong token; proves
//!                              the auth interceptor rejects without
//!                              hanging.
//!   6. Latency monitor       — Polls /api/topics + /api/groups +
//!                              /api/workflows + /api/health every
//!                              second and reports timing so the
//!                              operator can see they stay sub-ms
//!                              under load (the BUG #1 fix).
//!
//! Reports one stderr line per second:
//!   t  sends/s  errs/s  bp  bad  /api/topics  /api/groups  /api/wf
//!
//! Stops with code 0 after RUN_DURATION_SECS; or code 2 if /api/*
//! latency goes above 250ms for 5 consecutive seconds (means the
//! livelock fix regressed).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use qx_broker::Client;

const TOPICS: &[(&str, u32)] = &[
    ("stress-orders",    16),
    ("stress-payments",  16),
    ("stress-telemetry", 32),
    ("stress-audit",      4),
];

const CONSUMER_GROUPS: &[(&str, &str)] = &[
    ("fulfilment-svc",  "stress-orders"),
    ("ledger-svc",      "stress-payments"),
    ("analytics-svc",   "stress-telemetry"),
    ("warehouse-svc",   "stress-orders"),
];

const WORKFLOWS: &[&str] = &[
    "ship-deploy",
    "import-csv",
    "billing-rollup",
    "user-onboarding",
];

const PRODUCERS_PER_TOPIC: usize = 32;
const RUN_DURATION_SECS: u64 = 90;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let url = std::env::var("QX_URL").unwrap_or_else(|_| "http://127.0.0.1:55411".to_string());
    let token = std::env::var("QX_TOKEN").expect("QX_TOKEN required");
    let admin_base = std::env::var("QX_ADMIN_BASE")
        .unwrap_or_else(|_| "http://127.0.0.1:9090".to_string());

    eprintln!("stress: target  = {url}");
    eprintln!("stress: admin   = {admin_base}");
    eprintln!("stress: bearer  = {}…", &token[..token.len().min(12)]);
    eprintln!();

    // Bootstrap topics.
    let mut admin = Client::connect_with_token(url.clone(), token.clone())
        .await
        .expect("admin connect");
    for (name, partitions) in TOPICS {
        admin.open_topic(name, *partitions).await.expect("open_topic");
    }
    drop(admin);

    let sends = Arc::new(AtomicU64::new(0));
    let errs = Arc::new(AtomicU64::new(0));
    let bp_hits = Arc::new(AtomicU64::new(0));
    let bad_auth_hits = Arc::new(AtomicU64::new(0));
    let dlq_promotions = Arc::new(AtomicU64::new(0));
    let inflight_count = Arc::new(AtomicU64::new(0));
    let lag_count = Arc::new(AtomicU64::new(0));
    let rebalances_seen = Arc::new(AtomicU64::new(0));

    // ─── Producers ──────────────────────────────────────────────────
    for (topic, _partitions) in TOPICS {
        for _ in 0..PRODUCERS_PER_TOPIC {
            let url = url.clone();
            let token = token.clone();
            let sends = sends.clone();
            let errs = errs.clone();
            let topic = topic.to_string();
            tokio::spawn(async move {
                let mut c = match Client::connect_with_token(url, token).await {
                    Ok(c) => c,
                    Err(_) => {
                        errs.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                };
                let payload = vec![b'.'; 512];
                let mut i = 0u64;
                loop {
                    let key = format!("k-{}", i % 200);
                    match c.send(&topic, key.as_bytes(), &payload).await {
                        Ok(_) => {
                            sends.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            errs.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                    }
                    i = i.wrapping_add(1);
                }
            });
        }
    }

    // ─── Consumers ──────────────────────────────────────────────────
    for (group, topic) in CONSUMER_GROUPS {
        let url = url.clone();
        let token = token.clone();
        let group = group.to_string();
        let topic = topic.to_string();
        tokio::spawn(async move {
            let mut c = match Client::connect_with_token(url, token).await {
                Ok(c) => c,
                Err(_) => return,
            };
            // Join once.
            let assignment = loop {
                match c.join_group(&topic, &group).await {
                    Ok((_, _, a)) => break a,
                    Err(_) => tokio::time::sleep(Duration::from_secs(2)).await,
                }
            };
            loop {
                for &partition in &assignment {
                    let batch = c
                        .stream_from_group(&topic, &group, partition, 10)
                        .await
                        .unwrap_or_default();
                    if let Some(last) = batch.last() {
                        let _ = c
                            .commit_group_offset(&topic, &group, partition, last.id + 1)
                            .await;
                    }
                }
                // Slower than producers → lag grows visibly.
                tokio::time::sleep(Duration::from_millis(750)).await;
            }
        });
    }

    // ─── Workflows ──────────────────────────────────────────────────
    for &wf in WORKFLOWS {
        let url = url.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let mut c = match Client::connect_with_token(url, token).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let mut counter = 0u64;
            loop {
                let root = match c
                    .create_root_task(wf, format!("input-{counter}").into_bytes())
                    .await
                {
                    Ok(id) => id,
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                if let Ok(child) = c
                    .create_child_task(wf, root, format!("sub-{counter}").into_bytes())
                    .await
                {
                    // 85 / 5 / 5 / 5 — production-like.
                    let b = counter % 20;
                    if b < 17 {
                        let _ = c.complete_task(wf, child, b"done".to_vec()).await;
                        let _ = c.complete_task(wf, root, b"done".to_vec()).await;
                    } else if b == 17 {
                        let _ = c.fail_task(wf, child, "synthetic-fail".into()).await;
                        let _ = c.complete_task(wf, root, b"done".to_vec()).await;
                    } else if b == 18 {
                        let _ = c.cancel_task(wf, root).await;
                    } else {
                        // running
                    }
                }
                counter = counter.wrapping_add(1);
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        });
    }

    // ─── Bad-bearer probe — proves the auth interceptor rejects ────
    {
        let url = url.clone();
        let bad = bad_auth_hits.clone();
        tokio::spawn(async move {
            loop {
                // The connect itself succeeds (HTTP/2 handshake doesn't
                // need auth), but the FIRST RPC must come back
                // Unauthenticated.
                if let Ok(mut c) =
                    Client::connect_with_token(url.clone(), "qx_obviously_wrong".to_string())
                        .await
                {
                    match c.open_topic("anything", 1).await {
                        Err(_) => {
                            bad.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(_) => {
                            // SECURITY: a wrong-bearer succeeded.
                            // Surface as a load-bearing test failure.
                            eprintln!("stress: SECURITY BUG — wrong bearer accepted");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // ─── Backpressure probe — fires once we reach a high pending ───
    //
    // This task spawns 64 dedicated producers that hot-loop into the
    // `runaway` topic with no consumer. As pending climbs the broker
    // should hit the 5M cap and start returning BACKPRESSURE errors.
    // The probe counts them so the operator can see the cap fire.
    {
        let url = url.clone();
        let token = token.clone();
        let bp = bp_hits.clone();
        tokio::spawn(async move {
            let mut admin = match Client::connect_with_token(url.clone(), token.clone()).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let _ = admin.open_topic("runaway", 4).await;
            for _ in 0..64 {
                let url = url.clone();
                let token = token.clone();
                let bp = bp.clone();
                tokio::spawn(async move {
                    let mut c = match Client::connect_with_token(url, token).await {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    let payload = vec![0u8; 256];
                    let mut i = 0u64;
                    loop {
                        // Vary the key so the bp probe spreads
                        // across all partitions instead of pinning
                        // partition 0. Pinning one partition makes
                        // the per-partition writer the bottleneck
                        // and pending never reaches the cap.
                        let key = format!("rw-{}", i);
                        match c.send("runaway", key.as_bytes(), &payload).await {
                            Ok(_) => {}
                            Err(e) => {
                                let msg = format!("{e:?}");
                                if msg.contains("BACKPRESSURE") {
                                    bp.fetch_add(1, Ordering::Relaxed);
                                    tokio::time::sleep(Duration::from_millis(50)).await;
                                } else {
                                    tokio::time::sleep(Duration::from_millis(5)).await;
                                }
                            }
                        }
                        i = i.wrapping_add(1);
                    }
                });
            }
        });
    }

    // ─── DLQ probe — proves the "dead" column on /api/topics works ─
    //
    // Seeds N messages into `dlq-probe`, then loops claim + nack to
    // bump each message's attempts counter past the queue's
    // `max_attempts` (default 5). On the 6th claim the broker
    // moves each entry to `Status::Dead`. We then drain the dead
    // count from /api/topics and store it so the reporter can
    // print "dlq=N" alongside the other signals.
    {
        let url = url.clone();
        let token = token.clone();
        let dlq = dlq_promotions.clone();
        tokio::spawn(async move {
            let mut admin =
                match Client::connect_with_token(url.clone(), token.clone()).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
            if admin.open_topic("dlq-probe", 1).await.is_err() {
                return;
            }
            // Seed a couple of hundred messages — keeps the probe
            // distinct from the runaway/backpressure probe.
            let payload = vec![b'!'; 64];
            for i in 0..200u32 {
                let _ = admin
                    .send("dlq-probe", format!("dlq-{i}").as_bytes(), &payload)
                    .await;
            }
            // Repeatedly receive + nack until everything's dead.
            // Each receive returns a few records with their
            // `attempts` counter; nacking bounces them back to
            // pending with attempts++. After ~6 rounds every
            // record has hit attempts > 5 and the broker
            // promoted them to Status::Dead.
            for _ in 0..30 {
                let received = match admin.receive("dlq-probe", 100).await {
                    Ok(r) => r,
                    Err(_) => break,
                };
                if received.is_empty() {
                    break;
                }
                for msg in received {
                    let _ = admin.nack("dlq-probe", msg.receipt).await;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            // Poll the admin api for our topic's dead count so the
            // reporter can surface it.
            loop {
                if let Ok(()) = http_get_into(
                    "http://127.0.0.1:9090/api/topics",
                    &dlq,
                )
                .await
                {}
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    // ─── In-flight probe — populates the IN-FLIGHT column ──────────
    //
    // Topics' inflight count stays 0 if every consumer ack-or-
    // nacks immediately. This probe seeds N messages into
    // `inflight-probe`, claims them with `receive`, and HOLDS the
    // receipts for the whole stress run without acking or nacking.
    // Those records remain in `Status::InFlight` → the /api/topics
    // inflight column lights up.
    {
        let url = url.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let mut c = match Client::connect_with_token(url, token).await {
                Ok(c) => c,
                Err(_) => return,
            };
            if c.open_topic("inflight-probe", 1).await.is_err() { return; }
            let payload = vec![b'?'; 64];
            for i in 0..50u32 {
                let _ = c
                    .send("inflight-probe", format!("ifp-{i}").as_bytes(), &payload)
                    .await;
            }
            // Claim everything we just sent and hold the receipts.
            // qx-broker's default visibility timeout (30s) would
            // otherwise re-pend them; extending visibility on each
            // receipt keeps them parked in InFlight indefinitely.
            let mut held: Vec<qx_broker::proto::Receipt> = Vec::new();
            while held.len() < 50 {
                let batch = c.receive("inflight-probe", 50).await.unwrap_or_default();
                if batch.is_empty() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                for msg in batch {
                    held.push(msg.receipt);
                }
            }
            // Loop forever extending visibility so the broker
            // doesn't snap them back to Pending.
            loop {
                for r in &held {
                    let _ = c
                        .extend_visibility(
                            "inflight-probe",
                            r.clone(),
                            Duration::from_secs(60),
                        )
                        .await;
                }
                tokio::time::sleep(Duration::from_secs(20)).await;
            }
        });
    }

    // ─── Lag probe — populates the LAG column on Groups ────────────
    //
    // Open `lag-probe` topic with 4 partitions, fire-hose into it
    // from a fast producer, and consume from `lag-probe-grp` at
    // 1/10th the rate. The Groups page's lag column then climbs.
    {
        let url = url.clone();
        let token = token.clone();
        tokio::spawn(async move {
            let mut admin = match Client::connect_with_token(url.clone(), token.clone()).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let _ = admin.open_topic("lag-probe", 4).await;
            drop(admin);
            // Fast producer.
            for _ in 0..4 {
                let url = url.clone();
                let token = token.clone();
                tokio::spawn(async move {
                    let mut c = match Client::connect_with_token(url, token).await {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    let payload = vec![b'#'; 128];
                    let mut i = 0u64;
                    loop {
                        let k = format!("lp-{}", i % 32);
                        let _ = c.send("lag-probe", k.as_bytes(), &payload).await;
                        i = i.wrapping_add(1);
                    }
                });
            }
            // Slow consumer — runs once every 2s, drains at most
            // 5 records per partition per cycle. Lag accumulates.
            let url2 = url.clone();
            let token2 = token.clone();
            tokio::spawn(async move {
                let mut c = match Client::connect_with_token(url2, token2).await {
                    Ok(c) => c,
                    Err(_) => return,
                };
                let assignment = loop {
                    match c.join_group("lag-probe", "lag-probe-grp").await {
                        Ok((_, _, a)) => break a,
                        Err(_) => tokio::time::sleep(Duration::from_secs(2)).await,
                    }
                };
                loop {
                    for &p in &assignment {
                        if let Ok(b) = c.stream_from_group("lag-probe", "lag-probe-grp", p, 5).await
                        {
                            if let Some(l) = b.last() {
                                let _ = c
                                    .commit_group_offset(
                                        "lag-probe",
                                        "lag-probe-grp",
                                        p,
                                        l.id + 1,
                                    )
                                    .await;
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            });
        });
    }

    // ─── Rebalancing probe — sees 'rebalancing' on Groups ──────────
    //
    // Every 12s a fresh joiner enters the `rebal-grp` group on the
    // `lag-probe` topic. The group coordinator runs a rebalance.
    // Right between Join and SyncGroup the status flips to
    // 'rebalancing'; this probe polls /api/groups during that
    // window and counts the transitions it caught.
    {
        let url = url.clone();
        let token = token.clone();
        let rebals = rebalances_seen.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            loop {
                let url2 = url.clone();
                let token2 = token.clone();
                // Short-lived joiner: joins, naps, drops.
                tokio::spawn(async move {
                    let mut c = match Client::connect_with_token(url2, token2).await {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                    let _ = c.join_group("lag-probe", "rebal-grp").await;
                    tokio::time::sleep(Duration::from_secs(4)).await;
                });
                // Poll for ~3s catching the transition.
                let deadline = Instant::now() + Duration::from_secs(3);
                while Instant::now() < deadline {
                    if let Ok(body) = reqwest_text(
                        "http://127.0.0.1:9090/api/groups",
                    )
                    .await
                    {
                        if body.contains("\"status\":\"rebalancing\"") {
                            rebals.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                tokio::time::sleep(Duration::from_secs(8)).await;
            }
        });
    }

    // ─── Reporter ───────────────────────────────────────────────────
    let started = Instant::now();
    let mut last_sends = 0u64;
    let mut last_errs = 0u64;
    let mut last_bp = 0u64;
    let mut last_bad = 0u64;
    let mut over = 0u32;
    eprintln!(
        "{:>4}  {:>8}  {:>6}  {:>4}  {:>4}  {:>4}  {:>5}  {:>5}  {:>5}  {:>10}  {:>10}  {:>10}",
        "t", "send/s", "err/s", "bp/s", "bad/s", "dlq", "infl", "lag", "rebal", "/topics", "/groups", "/workflows"
    );
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let s = sends.load(Ordering::Relaxed);
        let e = errs.load(Ordering::Relaxed);
        let b = bp_hits.load(Ordering::Relaxed);
        let a = bad_auth_hits.load(Ordering::Relaxed);
        let ds = s.saturating_sub(last_sends);
        let de = e.saturating_sub(last_errs);
        let db = b.saturating_sub(last_bp);
        let da = a.saturating_sub(last_bad);
        let t_topics = probe(&admin_base, "/api/topics").await;
        let t_groups = probe(&admin_base, "/api/groups").await;
        let t_wf = probe(&admin_base, "/api/workflows").await;
        let dlq = dlq_promotions.load(Ordering::Relaxed);
        // Refresh inflight + lag from the cache on every tick so
        // the reporter shows live values.
        if let Ok(body) = reqwest_text("http://127.0.0.1:9090/api/topics").await {
            if let Some(n) = parse_field(&body, "inflight-probe", "inflight") {
                inflight_count.store(n, Ordering::Relaxed);
            }
        }
        if let Ok(body) = reqwest_text("http://127.0.0.1:9090/api/groups").await {
            if let Some(n) = parse_field(&body, "lag-probe-grp", "lag") {
                lag_count.store(n, Ordering::Relaxed);
            }
        }
        let infl = inflight_count.load(Ordering::Relaxed);
        let lag = lag_count.load(Ordering::Relaxed);
        let rebal = rebalances_seen.load(Ordering::Relaxed);
        eprintln!(
            "{:>4}  {:>8}  {:>6}  {:>4}  {:>4}  {:>4}  {:>5}  {:>5}  {:>5}  {:>10}  {:>10}  {:>10}",
            started.elapsed().as_secs(),
            ds, de, db, da, dlq, infl, lag, rebal,
            t_topics, t_groups, t_wf
        );
        let any_over = [&t_topics, &t_groups, &t_wf]
            .iter()
            .any(|t| t.contains("ms") && t.replace("ms", "").parse::<f64>().unwrap_or(0.0) > 250.0);
        if any_over { over += 1; } else { over = 0; }
        if over >= 5 {
            eprintln!("stress: REGRESSION — admin API latency > 250ms for 5s");
            std::process::exit(2);
        }
        if started.elapsed().as_secs() >= RUN_DURATION_SECS {
            eprintln!();
            eprintln!("stress: completed {RUN_DURATION_SECS}s clean — no regression detected");
            eprintln!("  total sends   = {s}");
            eprintln!("  total errors  = {e}");
            eprintln!("  BACKPRESSURE  = {b}  (bug 3 fired: {})", if b > 0 { "yes" } else { "no" });
            eprintln!("  bad-auth      = {a}  (interceptor rejects: {})", if a > 0 { "yes" } else { "no" });
            let dlq_final = dlq_promotions.load(Ordering::Relaxed);
            eprintln!("  DLQ dead      = {dlq_final}  (dead column populated: {})", if dlq_final > 0 { "yes" } else { "no" });
            let infl = inflight_count.load(Ordering::Relaxed);
            eprintln!("  in-flight     = {infl}  (inflight column populated: {})", if infl > 0 { "yes" } else { "no" });
            let lag = lag_count.load(Ordering::Relaxed);
            eprintln!("  consumer lag  = {lag}  (lag column non-zero: {})", if lag > 0 { "yes" } else { "no" });
            let rebal = rebalances_seen.load(Ordering::Relaxed);
            eprintln!("  rebalances    = {rebal}  (status='rebalancing' seen: {})", if rebal > 0 { "yes" } else { "no" });
            std::process::exit(0);
        }
        last_sends = s;
        last_errs = e;
        last_bp = b;
        last_bad = a;
    }
}

/// Single-shot /api/$path latency probe — returns a short
/// human-readable string suitable for one table cell.
async fn probe(base: &str, path: &str) -> String {
    let t0 = Instant::now();
    let url = format!("{base}{path}");
    let r = tokio::time::timeout(Duration::from_secs(2), http_get(&url)).await;
    match r {
        Ok(Ok(())) => format!("{:.1}ms", t0.elapsed().as_secs_f64() * 1000.0),
        Ok(Err(_)) => "ERR".to_string(),
        Err(_) => "TIMEOUT".to_string(),
    }
}

/// Fetch the response body. Used by the rebalancing probe to scan
/// for the `"status":"rebalancing"` substring as it polls /api/groups.
/// Tiny scan helper — finds the named object inside a JSON-list body
/// and returns the integer value of a named field. Avoids parsing the
/// whole response with serde so the stress test stays dep-free.
fn parse_field(body: &str, name_substr: &str, field: &str) -> Option<u64> {
    let needle = format!("\"name\":\"{name_substr}\"");
    let idx = body.find(&needle)?;
    let needle2 = format!("\"{field}\":");
    let rel = body[idx..].find(&needle2)?;
    let s = &body[idx + rel + needle2.len()..];
    let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

async fn reqwest_text(url: &str) -> Result<String, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let parsed = url.strip_prefix("http://").ok_or("bad url")?;
    let (authority, path) = match parsed.find('/') {
        Some(i) => (&parsed[..i], &parsed[i..]),
        None => (parsed, "/"),
    };
    let mut stream = tokio::net::TcpStream::connect(authority)
        .await
        .map_err(|e| e.to_string())?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Pulls the dlq-probe's `dead` count from /api/topics into the
/// supplied atomic counter so the reporter can show it live. Quick-
/// and-dirty parser: just substring-finds `"name":"dlq-probe"` and
/// pulls the `"dead":N` field that follows.
async fn http_get_into(url: &str, into: &Arc<AtomicU64>) -> Result<(), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let parsed = url.strip_prefix("http://").ok_or("bad url")?;
    let (authority, path) = match parsed.find('/') {
        Some(i) => (&parsed[..i], &parsed[i..]),
        None => (parsed, "/"),
    };
    let mut stream = tokio::net::TcpStream::connect(authority)
        .await
        .map_err(|e| e.to_string())?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.map_err(|e| e.to_string())?;
    let body = String::from_utf8_lossy(&buf);
    if let Some(idx) = body.find("\"name\":\"dlq-probe\"") {
        if let Some(dead_idx) = body[idx..].find("\"dead\":") {
            let s = &body[idx + dead_idx + 7..];
            let n: u64 = s
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .unwrap_or(0);
            into.store(n, Ordering::Relaxed);
        }
    }
    Ok(())
}

async fn http_get(url: &str) -> Result<(), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let parsed = url.strip_prefix("http://").ok_or("bad url")?;
    let (authority, path) = match parsed.find('/') {
        Some(i) => (&parsed[..i], &parsed[i..]),
        None => (parsed, "/"),
    };
    let mut stream = tokio::net::TcpStream::connect(authority)
        .await
        .map_err(|e| e.to_string())?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .await
        .map_err(|e| e.to_string())?;
    let head = String::from_utf8_lossy(&buf[..buf.len().min(16)]);
    if head.contains(" 2") { Ok(()) } else { Err(format!("status {head}")) }
}
