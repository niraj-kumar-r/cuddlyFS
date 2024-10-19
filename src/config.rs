use config::{Config, ConfigError, Environment, File};
use lazy_static::lazy_static;
use serde::Deserialize;
use std::env;

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = AppConfig::new().unwrap();
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct DatanodeConfig {
    pub namenode_rpc_address: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct NamenodeConfig {
    pub bind_address: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct AppConfig {
    pub debug: bool,
    pub namenode: NamenodeConfig,
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
            namenode: NamenodeConfig::default(),
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

impl Default for NamenodeConfig {
    fn default() -> Self {
        Self {
            bind_address: "http://localhost:50051".into(),
        }
    }
}
