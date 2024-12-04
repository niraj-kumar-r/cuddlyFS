use std::{
    collections::HashSet,
    net::SocketAddr,
    num::NonZero,
    str::FromStr,
    sync::{Mutex, RwLock},
};

use chrono::{DateTime, Utc};
use log::{debug, info};
use lru::LruCache;
use rand::{seq::SliceRandom, thread_rng};
use tokio::time;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    block::Block,
    cuddlyproto, datanode,
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
pub(super) struct DataRegistry {
    heartbeat_cache: Mutex<LruCache<Uuid, DateTime<Utc>>>,
    cancel_token: CancellationToken,
    block_to_datanodes: RwLock<KeyToDataAndIdMap<Uuid, Block, Uuid>>,
    datanode_to_blocks: RwLock<KeyToDataAndIdMap<Uuid, DatanodeInfo, Uuid>>,
    namenode_progress_tracker: RwLock<NamenodeProgressTracker>,
    fs_directory: RwLock<NamenodeState>,
    operation_logger: tokio::sync::Mutex<OperationLogger>,
    // start_time: DateTime<Utc>,
    // fsname_to_blocks: HashMap<FsName, BlockList>,
    // valid_blocks: HashSet<Block>,
    // block_manager: BlockManager,
    // datanode_manager: DatanodeManager,
    // lease_manager: LeaseManager,
}

impl DataRegistry {
    pub(super) fn new(cancel_token: CancellationToken) -> CuddlyResult<Self> {
        let data_registry = Self {
            // start_time: Utc::now(),
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
        storage_reports: Vec<cuddlyproto::StorageReportProto>,
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
                .put(Uuid::parse_str(&uuid).unwrap(), Utc::now());

            match r {
                Some(_previous_instant) => {
                    // info!(
                    //     "Updated heartbeat for {}. Previous heartbeat was at {:?}",
                    //     uuid, previous_instant
                    // );
                }
                None => {
                    info!("New Datanode Connected with uuid: {}", uuid);
                }
            }

            let mut datanode_to_blocks = self.datanode_to_blocks.write().unwrap();
            let datanode_info = DatanodeInfo {
                socket_address: datanode_registration
                    .datanode_id
                    .as_ref()
                    .and_then(|id| SocketAddr::from_str(&id.socket_addr).ok())
                    .unwrap(),

                datanode_uuid: Uuid::parse_str(&datanode_uuid.as_ref().unwrap()).unwrap(),
                total_capacity: storage_reports.iter().map(|report| report.capacity).sum(),
                used_capacity: storage_reports.iter().map(|report| report.dfs_used).sum(),
            };
            datanode_to_blocks.update_data(datanode_info.datanode_uuid, datanode_info);
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

    pub(crate) fn report_datanodes(&self) -> CuddlyResult<Vec<DatanodeInfo>> {
        Ok(self.get_alive_datanodes())
    }

    fn get_alive_datanodes(&self) -> Vec<DatanodeInfo> {
        let datanode_to_blocks = self.datanode_to_blocks.read().unwrap();
        self.heartbeat_cache
            .lock()
            .unwrap()
            .iter()
            .map(|(uuid, _instant)| datanode_to_blocks.get_data(uuid).unwrap().to_owned())
            .collect::<Vec<_>>()
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

    pub(crate) fn open_file(&self, path: &str) -> CuddlyResult<Vec<(Block, Vec<DatanodeInfo>)>> {
        let fs_directory = self.fs_directory.read().unwrap();
        let file_blocks = fs_directory.open_file(path)?;
        let block_to_datanodes = self.block_to_datanodes.read().unwrap();

        Ok(file_blocks
            .iter()
            .map(|block| {
                let datanodes = block_to_datanodes
                    .get_ids_for_key(&block.id)
                    .expect("If file with block exists, then this block should be replicated")
                    .iter()
                    .map(|s| {
                        self.datanode_to_blocks
                            .read()
                            .unwrap()
                            .get_data(s)
                            .expect("If block exists, then datanode should have it")
                            .clone()
                    })
                    .collect();
                (*block, datanodes)
            })
            .collect())
    }

    pub(crate) fn start_file_create(
        &self,
        path: &str,
    ) -> CuddlyResult<Option<(Block, Vec<DatanodeInfo>)>> {
        let fs_directory = self.fs_directory.read().unwrap();
        fs_directory.check_file_creation(path)?;
        self.namenode_progress_tracker
            .write()
            .unwrap()
            .add_file(path.to_owned())?;

        let mut target_nodes = HashSet::new();
        let mut available_nodes = self.get_alive_datanodes();
        debug!("Available nodes: {:?}", available_nodes);
        available_nodes.shuffle(&mut thread_rng());

        for node_info in available_nodes {
            debug!("Checking node: {:?}", node_info);
            if node_info.free_capacity() > APP_CONFIG.block_size {
                debug!("Node has enough capacity: {:?}", node_info.free_capacity());
                target_nodes.insert(node_info);
            }
            if target_nodes.len() as u64 >= APP_CONFIG.replication_factor as u64 {
                debug!("Found enough available nodes for file creation");
                let block_id = self.next_block_id();
                let seq = self
                    .namenode_progress_tracker
                    .write()
                    .unwrap()
                    .add_block(path, block_id)?;
                let block = Block::new(block_id, 0, seq);
                let mut blocks = HashSet::new();
                blocks.insert(block);
                debug!("Returning block: {:?}, targets: {:?}", block, target_nodes);
                return Ok(Some((block, target_nodes.into_iter().collect())));
            }
        }

        debug!("Not enough available nodes found for file creation");
        Ok(None)
    }

    fn next_block_id(&self) -> Uuid {
        let block_to_datanodes = self.block_to_datanodes.read().unwrap();
        let creates_in_progress = self.namenode_progress_tracker.read().unwrap();
        loop {
            let block_id = uuid::Uuid::new_v4();
            if block_to_datanodes.contains_key(&block_id)
                || creates_in_progress.contains_block(&block_id)
            {
                continue;
            } else {
                return block_id;
            }
        }
    }

    pub(crate) async fn finish_file_create(&self, path: &str) -> CuddlyResult<()> {
        let blocks = self.internal_finish_file_create(path)?;
        self.non_logging_finish_file(path, blocks.as_slice())?;
        self.log_operation(EditOperation::AddFile(path.to_owned(), blocks))
            .await;

        Ok(())
    }

    fn internal_finish_file_create(&self, path: &str) -> CuddlyResult<Vec<Block>> {
        let namenode_progress_tracker = self.namenode_progress_tracker.read().unwrap();
        let block_ids = namenode_progress_tracker.get_block_ids(path)?;
        for block_id in block_ids {
            let replication_count = namenode_progress_tracker.get_replication_count(*block_id);
            if replication_count < APP_CONFIG.replication_factor as u64 {
                return Err(CuddlyError::WaitingForReplication(format!(
                    "Block {} has been replicated only {} times, but {} replications are required",
                    block_id, replication_count, APP_CONFIG.replication_factor,
                )));
            }
        }

        let block_to_datanodes = self.block_to_datanodes.read().unwrap();
        let blocks = block_ids
            .iter()
            .map(|id| block_to_datanodes.get_data(id).unwrap().to_owned())
            .collect();
        Ok(blocks)
    }

    fn non_logging_finish_file(&self, path: &str, blocks: &[Block]) -> CuddlyResult<()> {
        let mut fs_directory = self.fs_directory.write().unwrap();
        fs_directory.create_file(path, blocks)?;
        let mut block_to_datanodes = self.block_to_datanodes.write().unwrap();
        for block in blocks {
            block_to_datanodes.insert_data(block.id, *block);
        }
        Ok(())
    }

    pub(crate) fn abort_file_create(&self, path: &str) -> CuddlyResult<()> {
        let mut namenode_progress_tracker = self.namenode_progress_tracker.write().unwrap();
        namenode_progress_tracker.remove_file(path)?;
        Ok(())
    }

    fn check_all_blocks_replicated(&self, path: &str) -> CuddlyResult<()> {
        let namenode_progress_tracker = self.namenode_progress_tracker.read().unwrap();
        let block_ids = namenode_progress_tracker.get_block_ids(path)?;
        for block_id in block_ids {
            let replication_count = self
                .namenode_progress_tracker
                .read()
                .unwrap()
                .get_replication_count(*block_id);
            if replication_count < APP_CONFIG.replication_factor as u64 {
                return Err(CuddlyError::WaitingForReplication(format!(
                    "Block {} has been replicated only {} times, but {} replications are required",
                    block_id, replication_count, APP_CONFIG.replication_factor,
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn start_another_block(
        &self,
        path: &str,
    ) -> CuddlyResult<Option<(Block, Vec<DatanodeInfo>)>> {
        self.check_all_blocks_replicated(path)?;

        let mut target_nodes = HashSet::new();
        let mut available_nodes = self.get_alive_datanodes();
        available_nodes.shuffle(&mut thread_rng());

        for node_info in available_nodes {
            if node_info.free_capacity() > APP_CONFIG.block_size {
                target_nodes.insert(node_info);
            }
            if target_nodes.len() as u64 >= APP_CONFIG.replication_factor as u64 {
                let block_id = self.next_block_id();
                let seq = self
                    .namenode_progress_tracker
                    .write()
                    .unwrap()
                    .add_block(path, block_id)?;
                let block = Block::new(block_id, 0, seq);
                return Ok(Some((block, target_nodes.into_iter().collect())));
            }
        }

        Ok(None)
    }

    pub(crate) fn abort_block(&self, path: &str, block: &Block) -> CuddlyResult<()> {
        let mut namenode_progress_tracker = self.namenode_progress_tracker.write().unwrap();
        namenode_progress_tracker.remove_block(path, block.id)?;
        Ok(())
    }
}
