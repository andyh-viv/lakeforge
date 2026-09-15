use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    forge_common::init_tracing("lakeforge-api");
    let config = lakeforge_api::Config::parse();
    lakeforge_api::run(config).await
}
