use log::info;

use crate::cuddlyproto;
use std::{collections::HashMap, sync::Mutex, time::Instant};

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
    start_time: Instant,
    heartbeats: Mutex<HashMap<String, Instant>>,
}

impl DataRegistry {
    pub(super) fn new() -> Self {
        Self {
            start_time: Instant::now(),
            heartbeats: Mutex::new(HashMap::new()),
        }
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
                .heartbeats
                .lock()
                .unwrap()
                .insert(uuid.clone(), Instant::now());

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

        response
    }
}
