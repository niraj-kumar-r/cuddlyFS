use std::{path::PathBuf, sync::Arc};

use crate::{
    config::APP_CONFIG,
    cuddlyproto::{self, heartbeat_service_client::HeartbeatServiceClient},
    errors::CuddlyResult,
};

use chrono::Utc;
use local_ip_address::local_ip;
use log::{error, info, warn};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use uuid::Uuid;

mod datanode_data_registry;
mod datanode_disk_info;

#[derive(Clone, Debug)]
pub struct Datanode {
    pub datanode_id: cuddlyproto::DatanodeIdProto,
    datanode_data_registry: Arc<datanode_data_registry::DatanodeDataRegistry>,
    cancel_token: CancellationToken,
    shutdown_send: mpsc::UnboundedSender<i8>,
}

impl Datanode {
    pub fn new(cancel_token: CancellationToken, shutdown_send: mpsc::UnboundedSender<i8>) -> Self {
        let datanode_uuid = Uuid::new_v4();
        Datanode {
            datanode_id: cuddlyproto::DatanodeIdProto {
                ip_addr: local_ip().unwrap().to_string(),
                host_name: hostname::get()
                    .unwrap_or_else(|_| "unknown".into())
                    .to_string_lossy()
                    .to_string(),
                datanode_uuid: datanode_uuid.to_string(),
                xfer_port: 50010,
                info_port: 50075,
                ipc_port: 50020,
                info_secure_port: 50070,
            },
            datanode_data_registry: Arc::new(
                datanode_data_registry::DatanodeDataRegistry::new(&PathBuf::from(format!(
                    "{}_{}",
                    APP_CONFIG.datanode.data_dir.display(),
                    datanode_uuid
                )))
                .unwrap(),
            ),
            cancel_token,
            shutdown_send,
        }
    }

    pub async fn run(self) -> CuddlyResult<()> {
        tokio::select! {
            _ = self.heartbeat_loop() => {},
            _ = self.cancel_token.cancelled() => {
                warn!("Heartbeat loop cancelled");
            },

        }

        Ok(())
    }

    async fn heartbeat_loop(&self) -> CuddlyResult<()> {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3));
        let mut consecutive_errors = 0;

        loop {
            interval.tick().await;
            match self.heartbeat().await {
                Ok(_) => {
                    info!("Heartbeat sent successfully");
                    consecutive_errors = 0;
                }
                Err(e) => {
                    warn!("Failed to send heartbeat: {:?}", e);
                    consecutive_errors += 1;

                    if consecutive_errors >= 5 {
                        error!("5 consecutive heartbeat failures, initiating shutdown...");
                        self.shutdown_send.send(1).unwrap();
                    }
                }
            }
        }
    }

    async fn get_heartbeat_client(uri: String) -> CuddlyResult<HeartbeatServiceClient<Channel>> {
        let client = HeartbeatServiceClient::connect(uri).await?;
        Ok(client)
    }

    pub async fn heartbeat(&self) -> CuddlyResult<tonic::Response<cuddlyproto::HeartbeatResponse>> {
        let req = tonic::Request::new(cuddlyproto::HeartbeatRequest {
            registration: Some(cuddlyproto::DatanodeRegistrationProto {
                datanode_id: Some(self.datanode_id.clone()),
                storage_info: Some(cuddlyproto::StorageInfoProto {
                    layout_version: 1,
                    namespace_id: 1,
                    cluster_id: 1.to_string(),
                    creation_time: Utc::now().timestamp() as u64,
                }),
                keys: Some(cuddlyproto::ExportedBlockKeysProto {
                    is_block_token_enabled: false,
                    key_update_interval: 1,
                    token_life_time: 1,
                    current_key: Some(cuddlyproto::BlockKeyProto {
                        key_id: 1,
                        expiry_date: 3000,
                        key_bytes: "my_secret_key".into(),
                    }),
                    all_keys: vec![],
                }),
                software_version: "0.1.0".to_string(),
            }),
            reports: vec![],
        });

        let mut client =
            Self::get_heartbeat_client(APP_CONFIG.datanode.namenode_rpc_address.clone()).await?;

        let response = client.heartbeat(req).await.unwrap();
        Ok(response)
    }
}
