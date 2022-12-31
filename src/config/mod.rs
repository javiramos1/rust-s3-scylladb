use color_eyre::Result;
use dotenv::dotenv;
use eyre::WrapErr;

use serde::Deserialize;
use tracing::{info, debug};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub host: String,
    pub port: i32,
    pub region: String,
    pub db_url: String,
    pub db_dc: String,
    pub parallel_files: usize,
    pub db_parallelism: usize,
    pub schema_file: String,
}

fn init_tracer() {
    #[cfg(debug_assertions)]
    let tracer = tracing_subscriber::fmt();
    #[cfg(not(debug_assertions))]
    let tracer = tracing_subscriber::fmt().json();

    tracer.with_env_filter(EnvFilter::from_default_env()).init();
}

impl Config {

    pub fn from_env() -> Result<Config> {
        dotenv().ok();

        init_tracer();

        info!("Loading configuration");

        let mut c = config::Config::new();

        c.merge(config::Environment::default())?;

        let config = c.try_into()
            .context("loading configuration from environment");

        debug!("Config: {:?}", config);
        config
    }
}
