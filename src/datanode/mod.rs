use std::sync::Arc;

use crate::{
    config::APP_CONFIG,
    cuddlyproto::{self},
    errors::{CuddlyError, CuddlyResult},
};

use chrono::Utc;
use datanode_data_handler::DatanodeDataHandler;
use local_ip_address::local_ip;
use log::{error, info, warn};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use uuid::Uuid;

use self::cuddlyproto::{node_service_client::NodeServiceClient, StorageReportProto};

mod datanode_client_service;
mod datanode_data_handler;
mod datanode_data_registry;
mod datanode_disk_info;

#[derive(Clone, Debug)]
pub struct Datanode {
    pub datanode_id: cuddlyproto::DatanodeIdProto,
    datanode_data_registry: Arc<datanode_data_registry::DatanodeDataRegistry>,
    node_service_client: NodeServiceClient<Channel>,
    cancel_token: CancellationToken,
    shutdown_send: mpsc::UnboundedSender<i8>,
}

impl Datanode {
    pub async fn new(
        cancel_token: CancellationToken,
        shutdown_send: mpsc::UnboundedSender<i8>,
    ) -> CuddlyResult<Self> {
        let datanode_uuid = Uuid::new_v4();
        Ok(Datanode {
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
            datanode_data_registry: Arc::new(datanode_data_registry::DatanodeDataRegistry::new(
                &APP_CONFIG.datanode.data_dir,
            )?),
            node_service_client: NodeServiceClient::connect(
                APP_CONFIG.datanode.namenode_rpc_address.clone(),
            )
            .await
            .map_err(|err| {
                CuddlyError::RPCError(format!("Could not connect to namenode: {}", err))
            })?,
            cancel_token,
            shutdown_send,
        })
    }

    pub async fn run(self) -> CuddlyResult<()> {
        let (received_block_tx, received_block_rx) = mpsc::channel::<cuddlyproto::Block>(8);
        tokio::select! {
            n_res = self.run_namenode_services(received_block_rx) => {
                if let Err(e) = n_res {
                    error!("Error running namenode services: {:?}", e);
                    return Err(CuddlyError::RPCError(e.to_string()));
                }
            },
            d_res = self.run_client_services(received_block_tx) => {
                if let Err(e) = d_res {
                    error!("Error running client services: {:?}", e);
                    return Err(CuddlyError::RPCError(e.to_string()));
                }
            },
            _ = self.cancel_token.cancelled() => {
                warn!("Datanode Shutting down...");
            },

        }

        Ok(())
    }

    fn get_node_service_client(&self) -> CuddlyResult<NodeServiceClient<Channel>> {
        Ok(self.node_service_client.clone())
    }

    async fn run_client_services(
        &self,
        received_block_tx: tokio::sync::mpsc::Sender<cuddlyproto::Block>,
    ) -> CuddlyResult<()> {
        let listener = TcpListener::bind(self.datanode_id.ip_addr.clone()).await?;

        loop {
            tokio::select! {
                incoming = listener.accept() => {
                    let (tcp_stream, _socket_addr) = incoming?;
                    let block_sender = received_block_tx.clone();
                    self.handle_incoming_request(tcp_stream, block_sender);
                }
                _ = self.cancel_token.cancelled() => {
                    warn!("Client service loop cancelled");
                    return Ok(());
                }
            }
        }
    }

    fn handle_incoming_request(
        &self,
        tcp_stream: TcpStream,
        block_sender: tokio::sync::mpsc::Sender<cuddlyproto::Block>,
    ) {
        let data_registry = Arc::clone(&self.datanode_data_registry);
        let packet_size = APP_CONFIG.packet_size;

        tokio::spawn(async move {
            let mut handler =
                DatanodeDataHandler::new(tcp_stream, data_registry, packet_size, block_sender);
            match handler.handle().await {
                Ok(()) => (),
                Err(e) => error!(
                    "An error occured while handling data server request: {:?}",
                    e
                ),
            }
        });
    }

    async fn run_namenode_services(
        &self,
        mut received_block_rx: tokio::sync::mpsc::Receiver<cuddlyproto::Block>,
    ) -> CuddlyResult<()> {
        let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(3));
        let mut consecutive_errors = 0;

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    match self.send_heartbeat().await {
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
                },
                block = received_block_rx.recv() => {
                    match self.handle_received_block(block).await {
                        Ok(_) => (),
                        Err(e) => error!("Failed to handle received block: {}", e)
                    }
                },
                _ = self.cancel_token.cancelled() => {
                    warn!("Datanode to Namenode service loop cancelled");
                    return Ok(());
                }
            }
        }
    }

    pub async fn send_heartbeat(
        &self,
    ) -> CuddlyResult<tonic::Response<cuddlyproto::HeartbeatResponse>> {
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
            reports: vec![StorageReportProto {
                storage: None,
                failed: false,
                capacity: self.datanode_data_registry.total().unwrap(),
                dfs_used: self.datanode_data_registry.used().unwrap(),
                remaining: self.datanode_data_registry.available().unwrap(),
                block_pool_used: 0,
                non_dfs_used: 0,
                mount: "/".to_string(),
            }],
        });

        let mut client = self.get_node_service_client()?;

        let response = client.heartbeat(req).await.unwrap();
        Ok(response)
    }

    async fn handle_received_block(&self, block: Option<cuddlyproto::Block>) -> CuddlyResult<()> {
        if let Some(block) = block {
            info!("New block received {:?}", block);
            let message = cuddlyproto::BlockReceivedRequest {
                address: self.datanode_id.ip_addr.to_string(),
                block: Some(block),
            };
            let mut client = self.get_node_service_client()?;
            tokio::spawn(async move {
                let request = tonic::Request::new(message);
                match client.block_received(request).await {
                    Ok(_) => (),
                    Err(e) => error!("Failed to notify namenode about the received block: {}", e),
                }
            });
        }
        Ok(())
    }
}
