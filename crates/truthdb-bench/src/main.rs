#[cfg(not(target_os = "linux"))]
compile_error!("truthdb-bench must be built for Linux targets. Use Docker or a Linux environment.");

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpStream;

use truthdb_net::{read_frame, write_frame};
use truthdb_proto::{
    CommandReq, CommandResp, Frame, HelloReq, HelloResp, MsgType, PROTOCOL_VERSION, decode_message,
    encode_message,
};

#[derive(Parser, Debug)]
#[command(name = "truthdb-bench")]
#[command(about = "Benchmark tool for TruthDB")]
#[command(version)]
struct Cli {
    /// Host of the TruthDB server.
    #[arg(long, default_value = "127.0.0.1", env = "TRUTHDB_HOST")]
    host: String,

    /// Port of the TruthDB server.
    #[arg(long, default_value_t = 9623, env = "TRUTHDB_PORT")]
    port: u16,

    /// Total operations per phase.
    #[arg(long, default_value_t = 100_000)]
    operations: u64,

    /// Number of concurrent TCP connections.
    #[arg(long, default_value_t = 1)]
    connections: u32,

    /// Skip the read phase.
    #[arg(long, default_value_t = false)]
    write_only: bool,

    /// Skip the write phase (assumes data exists).
    #[arg(long, default_value_t = false)]
    read_only: bool,

    /// Document size: small, medium, large.
    #[arg(long, default_value = "medium")]
    payload_size: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let addr = format!("{}:{}", cli.host, cli.port);

    eprintln!("=== TruthDB Benchmark ===");
    eprintln!(
        "target: {addr}, operations: {}, connections: {}, payload: {}",
        cli.operations, cli.connections, cli.payload_size
    );
    eprintln!();

    // Setup: connect one control connection to create the index
    if !cli.read_only {
        eprintln!("Setting up benchmark index...");
        let mut control = connect(&addr).await?;
        let setup_cmd = r#"create index bench {
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "category": { "type": "keyword" },
      "score": { "type": "float" }
    }
  }
}"#;
        let resp = send_command(&mut control, 0, setup_cmd).await?;
        if !resp.ok {
            // Index might already exist from a previous run, that's fine
            eprintln!("  index setup: {}", resp.message);
        } else {
            eprintln!("  index created");
        }
        drop(control);
    }

    // Write phase
    if !cli.read_only {
        eprintln!();
        eprintln!("Running write benchmark...");
        let results = run_phase(
            &addr,
            cli.connections,
            cli.operations,
            &cli.payload_size,
            Phase::Write,
        )
        .await?;
        print_results("Write", &results);
    }

    // Read phase
    if !cli.write_only {
        eprintln!();
        eprintln!("Running read benchmark...");
        let results = run_phase(
            &addr,
            cli.connections,
            cli.operations,
            &cli.payload_size,
            Phase::Read,
        )
        .await?;
        print_results("Read", &results);
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum Phase {
    Write,
    Read,
}

struct PhaseResults {
    total_ops: u64,
    duration_secs: f64,
    latencies_us: Vec<f64>,
    errors: u64,
    connections: u32,
}

async fn run_phase(
    addr: &str,
    num_connections: u32,
    total_ops: u64,
    payload_size: &str,
    phase: Phase,
) -> Result<PhaseResults> {
    let counter = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let payload_size = payload_size.to_string();

    let start = Instant::now();

    let mut handles = Vec::new();
    for _ in 0..num_connections {
        let addr = addr.to_string();
        let counter = counter.clone();
        let error_count = error_count.clone();
        let payload_size = payload_size.clone();

        let handle = tokio::spawn(async move {
            let mut latencies = Vec::new();
            let mut stream = match connect(&addr).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("  connection failed: {e}");
                    error_count.fetch_add(1, Ordering::Relaxed);
                    return latencies;
                }
            };

            let mut cmd_id: u64 = 1;
            loop {
                let op = counter.fetch_add(1, Ordering::Relaxed);
                if op >= total_ops {
                    break;
                }

                let command = match phase {
                    Phase::Write => generate_insert(op, &payload_size),
                    Phase::Read => generate_search(op, total_ops),
                };

                let req_start = Instant::now();
                match send_command(&mut stream, cmd_id, &command).await {
                    Ok(resp) => {
                        let elapsed_us = req_start.elapsed().as_secs_f64() * 1_000_000.0;
                        latencies.push(elapsed_us);
                        if !resp.ok {
                            error_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                cmd_id = cmd_id.wrapping_add(1);
            }

            latencies
        });
        handles.push(handle);
    }

    let mut all_latencies = Vec::new();
    for handle in handles {
        let mut latencies = handle.await?;
        all_latencies.append(&mut latencies);
    }

    let duration_secs = start.elapsed().as_secs_f64();
    let errors = error_count.load(Ordering::Relaxed);

    all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    Ok(PhaseResults {
        total_ops: all_latencies.len() as u64,
        duration_secs,
        latencies_us: all_latencies,
        errors,
        connections: num_connections,
    })
}

fn generate_insert(op: u64, payload_size: &str) -> String {
    let category = format!("cat-{:03}", op % 50);
    let score = (op % 1000) as f64 / 10.0;

    let title = match payload_size {
        "small" => format!("item {op}"),
        "large" => {
            let mut t = format!("benchmark document number {op} ");
            for i in 0..20 {
                t.push_str(&format!(
                    "additional filler text block {i} for realistic large payload sizing "
                ));
            }
            t
        }
        _ => format!(
            "benchmark document number {op} with additional descriptive text for realistic payload sizing"
        ),
    };

    format!(
        r#"insert document bench {{"title": "{title}", "category": "{category}", "score": {score}}}"#
    )
}

fn generate_search(op: u64, _total_ops: u64) -> String {
    if op.is_multiple_of(2) {
        let term = format!("item {}", op % 1000);
        format!(r#"search bench {{"query": {{"match": {{"title": "{term}"}}}}}}"#)
    } else {
        let category = format!("cat-{:03}", op % 50);
        format!(r#"search bench {{"query": {{"term": {{"category": "{category}"}}}}}}"#)
    }
}

fn print_results(phase_name: &str, results: &PhaseResults) {
    let ops_sec = if results.duration_secs > 0.0 {
        results.total_ops as f64 / results.duration_secs
    } else {
        0.0
    };

    println!();
    println!("  {phase_name} phase:");
    println!("    operations:  {}", results.total_ops);
    println!("    duration:    {:.2}s", results.duration_secs);
    println!("    throughput:  {:.0} ops/sec", ops_sec);
    println!("    connections: {}", results.connections);

    if !results.latencies_us.is_empty() {
        let min = results.latencies_us[0];
        let max = results.latencies_us[results.latencies_us.len() - 1];
        let p50 = percentile(&results.latencies_us, 50.0);
        let p95 = percentile(&results.latencies_us, 95.0);
        let p99 = percentile(&results.latencies_us, 99.0);
        let p999 = percentile(&results.latencies_us, 99.9);

        println!("    latency:");
        println!("      min:   {}", format_latency(min));
        println!("      p50:   {}", format_latency(p50));
        println!("      p95:   {}", format_latency(p95));
        println!("      p99:   {}", format_latency(p99));
        println!("      p999:  {}", format_latency(p999));
        println!("      max:   {}", format_latency(max));
    }

    if results.errors > 0 {
        println!("    errors:  {}", results.errors);
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn format_latency(us: f64) -> String {
    if us < 1000.0 {
        format!("{us:.0}µs")
    } else {
        format!("{:.2}ms", us / 1000.0)
    }
}

async fn connect(addr: &str) -> Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;

    let req = HelloReq {
        protocol_version: PROTOCOL_VERSION,
        client_name: "truthdb-bench".to_string(),
        client_version: env!("CARGO_PKG_VERSION").to_string(),
    };

    let frame = Frame {
        msg_type: MsgType::HelloReq,
        flags: 0,
        payload: encode_message(&req)?,
    };

    write_frame(&mut stream, &frame).await?;

    let resp_frame = read_frame(&mut stream).await?;
    if resp_frame.msg_type != MsgType::HelloResp {
        anyhow::bail!("unexpected response: {:?}", resp_frame.msg_type);
    }

    let _resp: HelloResp = decode_message(&resp_frame.payload)?;
    Ok(stream)
}

async fn send_command(stream: &mut TcpStream, id: u64, command: &str) -> Result<CommandResp> {
    let req = CommandReq {
        id,
        command: command.to_string(),
    };

    let frame = Frame {
        msg_type: MsgType::CommandReq,
        flags: 0,
        payload: encode_message(&req)?,
    };

    write_frame(stream, &frame).await?;

    let resp_frame = read_frame(stream).await?;
    if resp_frame.msg_type != MsgType::CommandResp {
        anyhow::bail!("unexpected response: {:?}", resp_frame.msg_type);
    }

    let resp: CommandResp = decode_message(&resp_frame.payload)?;
    Ok(resp)
}
