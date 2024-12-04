use std::{path::PathBuf, process::Command};

use chrono::{DateTime, Duration, Utc};
use log::info;

use crate::{errors::CuddlyResult, APP_CONFIG};

#[derive(Clone, Debug)]
pub(crate) struct DiskInfo {
    data_dir: PathBuf,
    used: u64,
    available: u64,
    last_update: DateTime<Utc>,
    update_interval: Duration,
}

#[allow(dead_code)]
impl DiskInfo {
    pub fn new(data_dir: &PathBuf) -> CuddlyResult<Self> {
        info!("Creating DiskInfo for {:?}", data_dir);
        let mut disk_info = Self {
            data_dir: data_dir.clone(),
            used: 0,
            available: 0,
            last_update: Utc::now(),
            update_interval: Duration::seconds(APP_CONFIG.datanode.disk_check_interval as i64),
        };
        if !disk_info.data_dir.exists() {
            std::fs::create_dir_all(disk_info.data_dir.as_path())?;
        }
        disk_info.refresh(true)?;
        Ok(disk_info)
    }

    fn refresh(&mut self, force: bool) -> CuddlyResult<()> {
        if force || self.needs_update() {
            let output = Command::new("df")
                .arg("-k")
                .arg("--output=used,avail")
                .arg(&self.data_dir)
                .output()?;

            let output = String::from_utf8_lossy(&output.stdout);
            let mut lines = output.lines();
            lines.next();
            let line = lines.next().unwrap();
            let mut columns = line.split_whitespace();
            self.used = columns.next().unwrap().parse().unwrap();
            self.available = columns.next().unwrap().parse().unwrap();
            self.last_update = Utc::now();
        }
        Ok(())
    }

    fn needs_update(&self) -> bool {
        Utc::now().signed_duration_since(self.last_update) > self.update_interval
    }

    pub(crate) fn get_used(&mut self) -> CuddlyResult<u64> {
        self.refresh(false)?;
        Ok(self.used)
    }

    pub(crate) fn get_available(&mut self) -> CuddlyResult<u64> {
        self.refresh(false)?;
        Ok(self.available)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disk_info() {
        let mut disk_info = DiskInfo::new(&PathBuf::from("/tmp/cuddlyfs/datanode")).unwrap();
        disk_info.refresh(true).unwrap();
        let used = disk_info.get_used().unwrap();
        let available = disk_info.get_available().unwrap();
        assert!(used > 0);
        assert!(available > 0);
    }
}
