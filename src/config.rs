use config::{Config, ConfigError, Environment, File};
use lazy_static::lazy_static;
use serde::Deserialize;
use std::{env, path::PathBuf};

lazy_static! {
    pub static ref APP_CONFIG: AppConfig = AppConfig::new().unwrap();
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct DatanodeConfig {
    pub namenode_rpc_address: String,
    pub data_dir: PathBuf,
    pub disk_check_interval: u64,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct NamenodeConfig {
    pub bind_address: String,
    pub name_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct AppConfig {
    pub debug: bool,
    pub namenode: NamenodeConfig,
    pub datanode: DatanodeConfig,
    pub block_size: u64,
    pub replication_factor: u8,
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
            block_size: 64 * 1024 * 1024,
            replication_factor: 3,
        }
    }
}

impl Default for DatanodeConfig {
    fn default() -> Self {
        Self {
            namenode_rpc_address: "http://[::1]:50051".into(),
            data_dir: std::env::temp_dir().join("cuddlyfs").join("datanode"),
            disk_check_interval: 3000,
        }
    }
}

impl Default for NamenodeConfig {
    fn default() -> Self {
        let mut namedir = std::env::temp_dir();
        namedir.push("cuddlyfs");
        namedir.push("namenode");

        Self {
            bind_address: "[::1]:50051".into(),
            name_dir: PathBuf::from(namedir),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.debug, false);
        assert_eq!(config.namenode.bind_address, "[::1]:50051");
        assert_eq!(config.datanode.namenode_rpc_address, "http://[::1]:50051");
    }
}
