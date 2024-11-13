use crate::block::Block;
use crate::config::AppConfig;
use crate::errors::CuddlyResult;

use std::fs::OpenOptions;
use std::io::SeekFrom;

use serde::{Deserialize, Serialize};

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt, BufStream};

use tracing::error;

/// A namenode modification.
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub enum EditOperation {
    Mkdir(String),
    AddFile(String, Vec<Block>),
}

/// OperationLogger is responsible to log all namenode modifications.
pub struct OperationLogger {
    old_log: BufStream<File>,
    new_log: BufStream<File>,
}

#[allow(dead_code)]
impl OperationLogger {
    /// Opens old edit logs (if exists) and creates new ones.
    pub fn open(config: &AppConfig) -> CuddlyResult<Self> {
        if !config.namenode.name_dir.exists() {
            std::fs::create_dir_all(config.namenode.name_dir.as_path())?;
        }
        let old_log = config.namenode.name_dir.join("edits");
        let old_log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(old_log)?;

        let new_log = config.namenode.name_dir.join("new-edits");
        let new_log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(new_log)?;

        Ok(Self {
            old_log: BufStream::new(File::from_std(old_log)),
            new_log: BufStream::new(File::from_std(new_log)),
        })
    }

    /// Logs a namenode modification by appending it to the log.
    /// If it cannot be appended, namenode crashes completely, to prevent
    /// any inconsistencies.
    pub async fn log_operation(&mut self, op: &EditOperation) {
        match self.non_exiting_log_operation(&op).await {
            Err(e) => {
                // TODO: Maybe we should panic here?
                error!(
                    "Failed to log operation '{:?}'.
                    Error: {:?}.This operation is not recoverable.
                    Exiting now.",
                    op, e
                );
                std::process::exit(1);
            }
            Ok(()) => (),
        };
    }

    async fn non_exiting_log_operation(&mut self, op: &EditOperation) -> CuddlyResult<()> {
        let op = serde_json::to_string(&op)? + "\n";
        self.new_log.write_all(op.as_bytes()).await?;
        self.new_log.flush().await?;
        Ok(())
    }

    /// Returns a list of EditOperations which are needed to restore
    /// the state of the namenode.
    pub async fn restore(&mut self) -> CuddlyResult<Vec<EditOperation>> {
        self.merge().await?;

        let mut ops = vec![];
        let mut buffer = String::new();
        loop {
            match self.old_log.read_line(&mut buffer).await? {
                0 => break,
                _ => {
                    let op: EditOperation = serde_json::from_str(&buffer)?;
                    ops.push(op);
                    buffer.clear();
                }
            }
        }

        Ok(ops)
    }

    /// Merges the new log stream into the old one. New one gets truncated.
    async fn merge(&mut self) -> CuddlyResult<()> {
        self.new_log.flush().await?;

        self.old_log.get_mut().seek(SeekFrom::End(0)).await?;
        self.new_log.get_mut().seek(SeekFrom::Start(0)).await?;

        tokio::io::copy(&mut self.new_log, &mut self.old_log).await?;

        self.new_log.get_mut().set_len(0).await?;
        self.new_log.get_mut().seek(SeekFrom::Start(0)).await?;
        self.old_log.get_mut().seek(SeekFrom::Start(0)).await?;

        Ok(())
    }
}
