//! `forge` — run Forge nodes, local clusters and SQL against a driver.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::net::SocketAddr;
use std::time::Duration;

use arrow::util::pretty::pretty_format_batches;
use clap::{Parser, Subcommand};
use forge_client::{ForgeClient, QueryEvent};
use forge_driver::DriverConfig;
use forge_executor::ExecutorConfig;

#[derive(Parser)]
#[command(name = "forge", version, about = "Forge distributed SQL engine")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a driver node.
    Driver {
        #[arg(long, default_value = "0.0.0.0:50051", env = "FORGE_DRIVER_BIND")]
        bind: SocketAddr,
        #[arg(long, default_value = "127.0.0.1", env = "FORGE_ADVERTISE_HOST")]
        advertise_host: String,
        #[arg(long, default_value = "/tmp/forge/driver", env = "FORGE_WORK_DIR")]
        work_dir: String,
        /// Fail queries instead of running them in-process when no executors are up.
        #[arg(long)]
        no_local_fallback: bool,
        #[arg(long, default_value = "30s", env = "FORGE_EXECUTOR_TIMEOUT")]
        executor_timeout: humantime_duration::Dur,
    },
    /// Run an executor node.
    Executor {
        #[arg(long, default_value = "0.0.0.0:50052", env = "FORGE_EXECUTOR_BIND")]
        bind: SocketAddr,
        #[arg(long, default_value = "127.0.0.1", env = "FORGE_ADVERTISE_HOST")]
        advertise_host: String,
        #[arg(long, default_value = "http://127.0.0.1:50051", env = "FORGE_DRIVER_ADDR")]
        driver: String,
        #[arg(long, env = "FORGE_EXECUTOR_ID")]
        id: Option<String>,
        #[arg(long, env = "FORGE_TASK_SLOTS")]
        slots: Option<usize>,
        #[arg(long, default_value = "/tmp/forge/executor", env = "FORGE_WORK_DIR")]
        work_dir: String,
    },
    /// Run a driver plus N executors in this process (development cluster).
    Local {
        #[arg(long, default_value_t = 2)]
        executors: usize,
        #[arg(long, default_value_t = 2)]
        slots: usize,
        #[arg(long, default_value_t = 50051)]
        port: u16,
        #[arg(long, default_value = "/tmp/forge/local")]
        work_dir: String,
    },
    /// Execute SQL (from -e, -f, or an interactive prompt).
    Sql {
        #[arg(long, default_value = "http://127.0.0.1:50051", env = "FORGE_DRIVER_ADDR")]
        driver: String,
        #[arg(short = 'e', long)]
        execute: Vec<String>,
        #[arg(short = 'f', long)]
        file: Option<String>,
        #[arg(long, default_value_t = 0)]
        max_rows: u64,
        /// Print rows as JSON lines instead of a table.
        #[arg(long)]
        json: bool,
        /// Print physical + distributed plan instead of running.
        #[arg(long)]
        explain: bool,
    },
    /// Register an external table on the driver.
    Register {
        #[arg(long, default_value = "http://127.0.0.1:50051", env = "FORGE_DRIVER_ADDR")]
        driver: String,
        name: String,
        #[arg(long)]
        format: String,
        #[arg(long)]
        location: String,
        #[arg(long = "opt", value_parser = parse_kv)]
        options: Vec<(String, String)>,
    },
    /// Show driver status.
    Status {
        #[arg(long, default_value = "http://127.0.0.1:50051", env = "FORGE_DRIVER_ADDR")]
        driver: String,
    },
    /// List executors.
    Executors {
        #[arg(long, default_value = "http://127.0.0.1:50051", env = "FORGE_DRIVER_ADDR")]
        driver: String,
    },
    /// List recent jobs, or show one job.
    Jobs {
        #[arg(long, default_value = "http://127.0.0.1:50051", env = "FORGE_DRIVER_ADDR")]
        driver: String,
        job_id: Option<String>,
    },
}

mod humantime_duration {
    use std::str::FromStr;
    use std::time::Duration;

    #[derive(Clone, Debug)]
    pub struct Dur(pub Duration);

    impl FromStr for Dur {
        type Err = String;
        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let s = s.trim();
            let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
            let n: u64 = num.parse().map_err(|e| format!("{e}"))?;
            let d = match unit {
                "" | "s" => Duration::from_secs(n),
                "ms" => Duration::from_millis(n),
                "m" => Duration::from_secs(n * 60),
                other => return Err(format!("unknown duration unit {other}")),
            };
            Ok(Dur(d))
        }
    }
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected key=value, got {s}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Driver { bind, advertise_host, work_dir, no_local_fallback, executor_timeout } => {
            forge_common::init_tracing("forge-driver");
            forge_driver::run(DriverConfig {
                bind,
                advertise_host,
                work_dir,
                local_fallback: !no_local_fallback,
                executor_timeout: executor_timeout.0,
                ..Default::default()
            })
            .await?;
        }
        Cmd::Executor { bind, advertise_host, driver, id, slots, work_dir } => {
            forge_common::init_tracing("forge-executor");
            let mut cfg = ExecutorConfig { bind, advertise_host, driver_addr: driver, work_dir, ..Default::default() };
            if let Some(id) = id {
                cfg.id = id;
            }
            if let Some(s) = slots {
                cfg.task_slots = s;
            }
            forge_executor::run(cfg).await?;
        }
        Cmd::Local { executors, slots, port, work_dir } => {
            forge_common::init_tracing("forge-local");
            run_local(executors, slots, port, work_dir).await?;
        }
        Cmd::Sql { driver, execute, file, max_rows, json, explain } => {
            let client = ForgeClient::connect(&driver).await?;
            let mut statements: Vec<String> = execute;
            if let Some(f) = file {
                statements.extend(split_statements(&std::fs::read_to_string(f)?));
            }
            if statements.is_empty() {
                repl(&client, max_rows, json, explain).await?;
            } else {
                for s in statements {
                    run_one(&client, &s, max_rows, json, explain).await?;
                }
            }
        }
        Cmd::Register { driver, name, format, location, options } => {
            let client = ForgeClient::connect(&driver).await?;
            let schema = client
                .register_table(&name, &format, &location, options.into_iter().collect::<HashMap<_, _>>())
                .await?;
            println!("registered {name}: {schema}");
        }
        Cmd::Status { driver } => {
            let client = ForgeClient::connect(&driver).await?;
            let s = client.status().await?;
            println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                "driver_id": s.driver_id, "version": s.version, "uptime_ms": s.uptime_ms,
                "executors": s.executors, "total_slots": s.total_slots, "free_slots": s.free_slots,
                "running_jobs": s.running_jobs,
            }))?);
        }
        Cmd::Executors { driver } => {
            let client = ForgeClient::connect(&driver).await?;
            for e in client.list_executors().await?.executors {
                let md = e.metadata.unwrap_or_default();
                let rs = e.resources.unwrap_or_default();
                println!(
                    "{:<24} {}:{:<6} slots={:<3} free={:<3} running={:<3} completed={:<6} failed={}",
                    md.id, md.host, md.port, md.task_slots, rs.free_task_slots, e.running_tasks, e.completed_tasks, e.failed_tasks
                );
            }
        }
        Cmd::Jobs { driver, job_id } => {
            let client = ForgeClient::connect(&driver).await?;
            match job_id {
                Some(id) => {
                    let j = client.get_job(&id).await?;
                    if let Some(job) = j.job {
                        println!("{} [{}] {}", job.job_id, job.state, job.sql);
                        if !job.error.is_empty() {
                            println!("error: {}", job.error);
                        }
                    }
                    for s in j.stages {
                        println!(
                            "stage {} [{}] tasks={} done={} running={} failed={} deps={:?}\n{}",
                            s.stage_id, s.state, s.num_partitions, s.completed, s.running, s.failed, s.depends_on, s.plan
                        );
                    }
                }
                None => {
                    for j in client.list_jobs(50).await? {
                        let p = j.progress.unwrap_or_default();
                        println!(
                            "{} [{:<9}] stages {}/{} tasks {}/{}  {}",
                            j.job_id, j.state, p.completed_stages, p.total_stages, p.completed_tasks, p.total_tasks,
                            j.sql.lines().next().unwrap_or("")
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

async fn run_local(executors: usize, slots: usize, port: u16, work_dir: String) -> forge_common::Result<()> {
    let driver_cfg = DriverConfig {
        bind: SocketAddr::from(([0, 0, 0, 0], port)),
        work_dir: format!("{work_dir}/driver"),
        ..Default::default()
    };
    let driver_addr = driver_cfg.advertise_addr();
    let mut handles = vec![tokio::spawn(forge_driver::run(driver_cfg))];
    tokio::time::sleep(Duration::from_millis(200)).await;
    for i in 0..executors {
        let cfg = ExecutorConfig {
            id: format!("local-{i}"),
            bind: SocketAddr::from(([0, 0, 0, 0], port + 1 + i as u16)),
            driver_addr: driver_addr.clone(),
            task_slots: slots,
            work_dir: format!("{work_dir}/executor-{i}"),
            heartbeat_interval: Duration::from_secs(2),
            ..Default::default()
        };
        handles.push(tokio::spawn(forge_executor::run(cfg)));
    }
    tracing::info!(driver = %driver_addr, executors, slots, "local cluster up");
    for h in handles {
        h.await.map_err(|e| forge_common::ForgeError::Internal(e.to_string()))??;
    }
    Ok(())
}

async fn repl(client: &ForgeClient, max_rows: u64, json: bool, explain: bool) -> forge_common::Result<()> {
    let stdin = std::io::stdin();
    let mut buf = String::new();
    loop {
        print!("{}", if buf.is_empty() { "forge> " } else { "    -> " });
        std::io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        let trimmed = line.trim();
        if buf.is_empty() && matches!(trimmed, "quit" | "exit" | "\\q") {
            break;
        }
        buf.push_str(&line);
        if trimmed.ends_with(';') {
            let stmt = std::mem::take(&mut buf);
            if let Err(e) = run_one(client, stmt.trim().trim_end_matches(';'), max_rows, json, explain).await {
                eprintln!("error: {e}");
            }
        }
    }
    Ok(())
}

fn split_statements(text: &str) -> Vec<String> {
    text.split(';')
        .map(|s| s.lines().filter(|l| !l.trim_start().starts_with("--")).collect::<Vec<_>>().join("\n"))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

async fn run_one(client: &ForgeClient, sql: &str, max_rows: u64, json: bool, explain: bool) -> forge_common::Result<()> {
    if explain {
        let e = client.explain(sql, false).await?;
        println!("== Logical ==\n{}\n== Physical ==\n{}\n== Distributed ==\n{}", e.logical_plan, e.physical_plan, e.distributed_plan);
        return Ok(());
    }
    let mut stream = client.sql_stream_with(sql, HashMap::new(), max_rows).await?;
    let mut batches = Vec::new();
    let mut finished = None;
    while let Some(ev) = stream.next().await? {
        match ev {
            QueryEvent::Started(s) => tracing::debug!(job = %s.job_id, "started"),
            QueryEvent::Progress(p) => {
                eprint!(
                    "\rstages {}/{} tasks {}/{} running {} failed {}    ",
                    p.completed_stages, p.total_stages, p.completed_tasks, p.total_tasks, p.running_tasks, p.failed_tasks
                );
            }
            QueryEvent::Schema(_) => {}
            QueryEvent::Batch(b) => batches.push(b),
            QueryEvent::Finished(f) => finished = Some(f),
        }
    }
    eprint!("\r");
    if json {
        for b in &batches {
            let mut w = arrow::json::LineDelimitedWriter::new(std::io::stdout());
            w.write(b)?;
            w.finish()?;
        }
    } else if !batches.is_empty() {
        println!("{}", pretty_format_batches(&batches)?);
    }
    if let Some(f) = finished {
        eprintln!("{} row(s) in {} ms{}", f.rows, f.elapsed_ms, if f.stages.is_empty() { String::new() } else { format!(", {} stage(s)", f.stages.len()) });
    }
    Ok(())
}
