use std::{path::PathBuf, time::Duration};

use chrono::{DateTime, Utc};

use crate::{errors::CuddlyResult, APP_CONFIG};

pub(crate) struct DiskInfo {
    data_dir: PathBuf,
    used: u64,
    available: u64,
    last_update: DateTime<Utc>,
    update_interval: Duration,
}

impl DiskInfo {
    pub fn new() -> CuddlyResult<Self> {
        let disk_info = Self {
            data_dir: APP_CONFIG.datanode.data_dir.clone(),
            used: 0,
            available: 0,
            last_update: Utc::now(),
            update_interval: Duration::from_secs(APP_CONFIG.datanode.disk_check_interval),
        };

        Ok(disk_info)
    }
}
