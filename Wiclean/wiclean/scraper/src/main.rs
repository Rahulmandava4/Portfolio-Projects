use std::time::Duration;

use clap::{Parser, Subcommand};
use deadpool_postgres::{Config, Pool, Runtime};
use ingest::temp_db::TempDb;
#[allow(unused)]
use jemallocator::Jemalloc;
#[allow(unused)]
use mimalloc::MiMalloc;
use tokio::runtime;
use tokio_postgres::NoTls;

mod ingest;
mod postprocess_to_parquet;

#[global_allocator]
static ALLOC: Jemalloc = Jemalloc;

#[derive(Debug, Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Ingest,
    PostprocessToParquet,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let (temp_db, temp_db_shutdown) = TempDb::open()?;

    let runtime = runtime::Builder::new_multi_thread().enable_all().build()?;
    let _guard = runtime.enter();
    let _db_pool = init_db();

    let cli = Cli::parse();
    match cli.command {
        Command::Ingest => runtime.block_on(ingest::ingest(temp_db.clone()))?,
        Command::PostprocessToParquet => postprocess_to_parquet::postprocess_to_parquet(&temp_db)?,
    }

    drop(_guard);

    tracing::info!("shutting down");
    drop(temp_db);
    runtime.shutdown_timeout(Duration::from_secs(60 * 60));
    temp_db_shutdown.recv().unwrap();
    Ok(())
}

fn init_db() -> Pool {
    let mut cfg = Config::new();
    cfg.host = Some("127.0.0.1".into());
    cfg.dbname = Some("wiclean".into());
    cfg.user = Some("wiclean".into());
    cfg.password = Some("wiclean".into());
    cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap()
}
