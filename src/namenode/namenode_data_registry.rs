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

use self::cuddlyproto::StatusEnum;
use crate::{block::Block, cuddlyproto, utils::key_to_data_and_id_map::KeyToDataAndIdMap};

use super::datanode_info::DatanodeInfo;

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
    // fsname_to_blocks: HashMap<FsName, BlockList>,
    // valid_blocks: HashSet<Block>,
    // block_manager: BlockManager,
    // datanode_manager: DatanodeManager,
    // lease_manager: LeaseManager,
    // fs_directory: FSDirectory,
    // edit_log: FSEditLog,
}

impl DataRegistry {
    pub(super) fn new(cancel_token: CancellationToken) -> Self {
        let data_registry = Self {
            start_time: Utc::now(),
            heartbeat_cache: Mutex::new(LruCache::new(NonZero::new(CACHE_SIZE).unwrap())),
            block_to_datanodes: RwLock::new(KeyToDataAndIdMap::new()),
            datanode_to_blocks: RwLock::new(KeyToDataAndIdMap::new()),
            cancel_token,
        };

        data_registry
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

    pub(crate) fn block_received(&self, address: &str, block: &Block) -> Result<(), StatusEnum> {
        info!("Block received from address: {}, {}", address, block);

        Ok(())
    }
}
