//! Closed-loop load for the pilot's benchmark: `--concurrency` workers, each
//! sending the next path from one shared list and reading the whole response
//! before its next request. Python and Rust get the same list in the same
//! order from the same starting point, so they answer the same questions.
//!
//! ```text
//! ks-loadgen --target http://host:8090 --paths paths.txt \
//!     --concurrency 8 --warmup 5 --duration 30 --label rust \
//!     --header 'Cookie: dashboard_session=…' --rotate-real-ip
//! ```
//!
//! `--header` is sent with every request; `--rotate-real-ip` gives each
//! request an `X-Real-IP` of its own, so a per-client rate limit is checked
//! on both services and trips on neither.
//!
//! Prints one JSON object: throughput, latency percentiles (nearest rank,
//! microsecond resolution), status counts and transport errors.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use http_body_util::{BodyExt, Empty};
use hyper::Request;
use hyper::body::Bytes;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

struct Args {
    target: String,
    paths: Vec<String>,
    concurrency: usize,
    warmup: Duration,
    duration: Duration,
    label: String,
    headers: Vec<(String, String)>,
    rotate_real_ip: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = std::env::args().skip(1);
    let (mut target, mut paths_file, mut label) = (None, None, String::from("run"));
    let (mut concurrency, mut warmup, mut duration) = (1_usize, 5.0_f64, 30.0_f64);
    let (mut headers, mut rotate_real_ip) = (Vec::new(), false);
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or(format!("{flag} needs a value"));
        match flag.as_str() {
            "--target" => target = Some(value()?.trim_end_matches('/').to_owned()),
            "--paths" => paths_file = Some(value()?),
            "--label" => label = value()?,
            "--concurrency" => concurrency = value()?.parse().map_err(|_| "--concurrency")?,
            "--warmup" => warmup = value()?.parse().map_err(|_| "--warmup")?,
            "--duration" => duration = value()?.parse().map_err(|_| "--duration")?,
            "--header" => {
                let header = value()?;
                let (name, v) = header
                    .split_once(':')
                    .ok_or(format!("--header {header:?} is not `Name: value`"))?;
                headers.push((name.trim().to_owned(), v.trim().to_owned()));
            }
            "--rotate-real-ip" => rotate_real_ip = true,
            other => return Err(format!("unknown flag {other}")),
        }
    }
    let paths_file = paths_file.ok_or("--paths is required")?;
    let paths: Vec<String> = std::fs::read_to_string(&paths_file)
        .map_err(|e| format!("{paths_file}: {e}"))?
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
        .collect();
    if paths.is_empty() || concurrency == 0 {
        return Err("need at least one path and one worker".into());
    }
    Ok(Args {
        target: target.ok_or("--target is required")?,
        paths,
        concurrency,
        warmup: Duration::from_secs_f64(warmup),
        duration: Duration::from_secs_f64(duration),
        label,
        headers,
        rotate_real_ip,
    })
}

#[derive(Default)]
struct Tally {
    micros: Vec<u64>,
    statuses: BTreeMap<u16, u64>,
    errors: u64,
}

fn percentile(sorted: &[u64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1] as f64 / 1000.0
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(error) => {
            eprintln!("ks-loadgen: {error}");
            return ExitCode::from(2);
        }
    };
    let client: Client<_, Empty<Bytes>> = Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(args.concurrency)
        .build_http();
    let uris: Arc<Vec<hyper::Uri>> = match args
        .paths
        .iter()
        .map(|p| format!("{}{p}", args.target).parse())
        .collect()
    {
        Ok(uris) => Arc::new(uris),
        Err(error) => {
            eprintln!("ks-loadgen: a path does not make a URI: {error}");
            return ExitCode::from(2);
        }
    };
    let next = Arc::new(AtomicUsize::new(0));
    let measure_from = Instant::now() + args.warmup;
    let stop_at = measure_from + args.duration;

    let workers: Vec<_> = (0..args.concurrency)
        .map(|_| {
            let (client, uris, next) = (client.clone(), uris.clone(), next.clone());
            let (headers, rotate) = (args.headers.clone(), args.rotate_real_ip);
            tokio::spawn(async move {
                let mut tally = Tally::default();
                loop {
                    let started = Instant::now();
                    if started >= stop_at {
                        return tally;
                    }
                    let n = next.fetch_add(1, Ordering::Relaxed);
                    let mut builder = Request::get(uris[n % uris.len()].clone());
                    for (name, value) in &headers {
                        builder = builder.header(name.as_str(), value.as_str());
                    }
                    if rotate {
                        let ip = format!("10.{}.{}.{}", (n >> 16) & 255, (n >> 8) & 255, n & 255);
                        builder = builder.header("x-real-ip", ip);
                    }
                    let request = builder.body(Empty::new()).expect("a GET with no body");
                    let outcome = async {
                        let response = client.request(request).await.map_err(|e| e.to_string())?;
                        let status = response.status().as_u16();
                        response
                            .into_body()
                            .collect()
                            .await
                            .map_err(|e| e.to_string())?;
                        Ok::<u16, String>(status)
                    }
                    .await;
                    if started < measure_from {
                        continue;
                    }
                    match outcome {
                        Ok(status) => {
                            tally.micros.push(started.elapsed().as_micros() as u64);
                            *tally.statuses.entry(status).or_default() += 1;
                        }
                        Err(_) => tally.errors += 1,
                    }
                }
            })
        })
        .collect();

    let mut total = Tally::default();
    for worker in workers {
        let tally = worker.await.expect("a worker does not panic");
        total.micros.extend(tally.micros);
        total.errors += tally.errors;
        for (status, n) in tally.statuses {
            *total.statuses.entry(status).or_default() += n;
        }
    }
    let seconds = measure_from.elapsed().as_secs_f64();
    total.micros.sort_unstable();
    let n = total.micros.len();
    let summary = serde_json::json!({
        "label": args.label,
        "concurrency": args.concurrency,
        "requests": n,
        "seconds": (seconds * 100.0).round() / 100.0,
        "rps": ((n as f64 / seconds) * 10.0).round() / 10.0,
        "p50_ms": percentile(&total.micros, 0.50),
        "p90_ms": percentile(&total.micros, 0.90),
        "p95_ms": percentile(&total.micros, 0.95),
        "p99_ms": percentile(&total.micros, 0.99),
        "max_ms": percentile(&total.micros, 1.0),
        "statuses": total.statuses,
        "errors": total.errors,
    });
    println!("{summary}");
    if total.errors > 0 || n == 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
