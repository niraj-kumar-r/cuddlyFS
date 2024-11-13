use std::{
    num::NonZero,
    sync::{Mutex, RwLock},
};

use chrono::{DateTime, Utc};
use log::info;
use lru::LruCache;
use tokio::time;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    block::Block,
    cuddlyproto,
    errors::{CuddlyError, CuddlyResult},
    utils::key_to_data_and_id_map::KeyToDataAndIdMap,
    APP_CONFIG,
};

use super::{
    datanode_info::DatanodeInfo,
    namenode_operation_logger::{EditOperation, OperationLogger},
    namenode_progress_tracker::NamenodeProgressTracker,
    namenode_state::NamenodeState,
};

// Create a const for cache size
const CACHE_SIZE: usize = 100;
const HEARTBEAT_TIMEOUT: i64 = 3 * 200;
const HEARTBEAT_RECHECK_INTERVAL: u64 = 20;

/**
 * FSNamesystem is a container of both transient
 * and persisted name-space state, and does all the book-keeping
 * work on a NameNode.
 *
 * Its roles are briefly described below:
 *
 * 1) Is the container for BlockManager, DatanodeManager,
 *    DelegationTokens, LeaseManager, etc. services.
 * 2) RPC calls that modify or inspect the name-space
 *    should get delegated here.
 * 3) Anything that touches only blocks (eg. block reports),
 *    it delegates to BlockManager.
 * 4) Anything that touches only file information (eg. permissions, mkdirs),
 *    it delegates to FSDirectory.
 * 5) Anything that crosses two of the above components should be
 *    coordinated here.
 * 6) Logs mutations to FSEditLog.
 *
 * This class and its contents keep:
 *
 * 1)  Valid fsname {@literal -->} blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block {@literal -->} machinelist (kept in memory, rebuilt dynamically
 *     from reports)
 * 4)  machine {@literal -->} blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 */
#[derive(Debug)]
#[allow(dead_code)]
pub(super) struct DataRegistry {
    start_time: DateTime<Utc>,
    heartbeat_cache: Mutex<LruCache<String, DateTime<Utc>>>,
    cancel_token: CancellationToken,
    block_to_datanodes: RwLock<KeyToDataAndIdMap<Uuid, Block, Uuid>>,
    datanode_to_blocks: RwLock<KeyToDataAndIdMap<Uuid, DatanodeInfo, Uuid>>,
    namenode_progress_tracker: RwLock<NamenodeProgressTracker>,
    fs_directory: RwLock<NamenodeState>,
    operation_logger: tokio::sync::Mutex<OperationLogger>,
    // fsname_to_blocks: HashMap<FsName, BlockList>,
    // valid_blocks: HashSet<Block>,
    // block_manager: BlockManager,
    // datanode_manager: DatanodeManager,
    // lease_manager: LeaseManager,
}

impl DataRegistry {
    pub(super) fn new(cancel_token: CancellationToken) -> CuddlyResult<Self> {
        let data_registry = Self {
            start_time: Utc::now(),
            heartbeat_cache: Mutex::new(LruCache::new(NonZero::new(CACHE_SIZE).unwrap())),
            block_to_datanodes: RwLock::new(KeyToDataAndIdMap::new()),
            datanode_to_blocks: RwLock::new(KeyToDataAndIdMap::new()),
            namenode_progress_tracker: RwLock::new(NamenodeProgressTracker::new()),
            fs_directory: RwLock::new(NamenodeState::new()),
            operation_logger: tokio::sync::Mutex::new(OperationLogger::open(&APP_CONFIG)?),
            cancel_token,
        };

        Ok(data_registry)
    }

    pub(crate) async fn run(&self) {
        tokio::select! {
            _ = self.cancel_token.cancelled() => {
                info!("DataRegistry cancelled");
            }
            _ = self.do_heartbeat_monitoring() => {
                info!("Heartbeat monitor finished");
            }
        }

        info!("DataRegistry run finished");
    }

    pub fn handle_heartbeat(
        &self,
        datanode_registration: cuddlyproto::DatanodeRegistrationProto,
        _storage_reports: Vec<cuddlyproto::StorageReportProto>,
    ) -> cuddlyproto::HeartbeatResponse {
        let datanode_uuid = datanode_registration
            .datanode_id
            .as_ref()
            .map(|id| id.datanode_uuid.clone());

        if let Some(uuid) = &datanode_uuid {
            let r = self
                .heartbeat_cache
                .lock()
                .unwrap()
                .put(uuid.clone(), Utc::now());

            match r {
                Some(previous_instant) => {
                    info!(
                        "Updated heartbeat for {}. Previous heartbeat was at {:?}",
                        uuid, previous_instant
                    );
                }
                None => {
                    info!("New Datanode Connected with uuid: {}", uuid);
                }
            }
            let response = cuddlyproto::HeartbeatResponse {
                status: Some(cuddlyproto::StatusCode {
                    success: true,
                    code: cuddlyproto::StatusEnum::Ok as i32,
                    message: "Ok".to_string(),
                }),
                ha_status: Some(cuddlyproto::NnhaStatusHeartbeatProto {
                    state: cuddlyproto::nnha_status_heartbeat_proto::State::Active as i32,
                    txid: uuid::Uuid::new_v4().to_string(),
                }),
            };

            return response;
        } else {
            info!("Datanode registration failed, request did not contain a UUID");

            let response = cuddlyproto::HeartbeatResponse {
                status: Some(cuddlyproto::StatusCode {
                    success: false,
                    code: cuddlyproto::StatusEnum::EInval as i32,
                    message: "Request doesn't have UUID of node".to_string(),
                }),
                ha_status: Some(cuddlyproto::NnhaStatusHeartbeatProto {
                    state: cuddlyproto::nnha_status_heartbeat_proto::State::Active as i32,
                    txid: uuid::Uuid::new_v4().to_string(),
                }),
            };

            return response;
        }
    }

    fn remove_invalid_datanodes(&self) {
        let mut cache = self.heartbeat_cache.lock().unwrap();
        let now = Utc::now();
        let mut to_remove = Vec::new();

        for (uuid, instant) in cache.iter() {
            if now.signed_duration_since(*instant).num_seconds() > HEARTBEAT_TIMEOUT {
                to_remove.push(uuid.clone());
            }
        }

        for uuid in to_remove {
            cache.pop(&uuid);
            info!(
                "Removed Datanode with uuid (did not receive heartbeat): {}",
                uuid
            );
        }
    }

    async fn do_heartbeat_monitoring(&self) {
        let mut heartbeat_tick =
            time::interval(time::Duration::from_secs(HEARTBEAT_RECHECK_INTERVAL));
        loop {
            heartbeat_tick.tick().await;
            self.remove_invalid_datanodes();
        }
    }

    pub(crate) fn block_received(&self, node_id: &str, block: &Block) -> CuddlyResult<()> {
        info!("Block received from node_id: {}, {}", node_id, block);

        let datanode_uuid = match Uuid::parse_str(node_id) {
            Ok(datanode_uuid) => datanode_uuid,
            Err(_) => return Err(CuddlyError::FSError(format!("Invalid UUID: {}", node_id))),
        };

        let new_reported = {
            let mut block_to_datanodes = self.block_to_datanodes.write().unwrap();
            block_to_datanodes.insert_id_for_key(block.id, *block, datanode_uuid)
        };

        if new_reported {
            let mut progress_tracker = self.namenode_progress_tracker.write().unwrap();
            progress_tracker.increment_replication(block.id);
        }

        let insert_id_success = {
            let mut datanode_to_blocks = self.datanode_to_blocks.write().unwrap();
            datanode_to_blocks.insert_id_for_key_if_present(datanode_uuid, block.id)
        };

        if !insert_id_success {
            return Err(CuddlyError::FSError(format!(
                "Block received from unregistered datanode '{}'.",
                node_id
            )));
        }

        Ok(())
    }

    async fn log_operation(&self, op: EditOperation) {
        let mut edit_logger = self.operation_logger.lock().await;
        edit_logger.log_operation(&op).await;
    }

    pub(crate) async fn make_dir(&self, path: &str) -> CuddlyResult<()> {
        self.non_logging_make_dir(path)?;
        self.log_operation(EditOperation::Mkdir(path.to_owned()))
            .await;
        Ok(())
    }

    fn non_logging_make_dir(&self, path: &str) -> CuddlyResult<()> {
        let mut fs_directory = self.fs_directory.write().unwrap();
        fs_directory.make_dir(path)
    }

    pub(crate) fn list(&self, path: &str) -> CuddlyResult<Vec<String>> {
        let fs_directory = self.fs_directory.read().unwrap();
        Ok(fs_directory
            .list(path)?
            .iter()
            .map(|s| String::from(*s))
            .collect())
    }
}
