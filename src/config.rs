use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;

#[derive(Debug, Clone, Deserialize)]
#[allow(unused)]
pub struct DatanodeConfig {
    pub namenode_rpc_address: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct AppConfig {
    pub debug: bool,
    pub datanode: DatanodeConfig,
}

impl AppConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let settings = Config::builder()
            .add_source(File::with_name("config/default"))
            .add_source(File::with_name(&format!("config/{}", run_mode)).required(false))
            .add_source(File::with_name("config/local").required(false))
            .add_source(Environment::with_prefix("CUDDLYFS"))
            .build()?;

        settings.try_deserialize()
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            debug: false,
            datanode: DatanodeConfig::default(),
        }
    }
}

impl Default for DatanodeConfig {
    fn default() -> Self {
        Self {
            namenode_rpc_address: "http://localhost:50051".into(),
        }
    }
}
