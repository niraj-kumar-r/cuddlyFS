use log::info;
use namenode_data_registry::DataRegistry;
use namenode_heartbeat_service::NamenodeHeartbeatService;
use namenode_node_service::NamenodeNodeService;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::{
    cuddlyproto::{
        heartbeat_service_server::HeartbeatServiceServer, node_service_server::NodeServiceServer,
    },
    errors::CuddlyResult,
};

mod datanode_info;
mod namenode_data_registry;
mod namenode_heartbeat_service;
mod namenode_node_service;
mod namenode_progress_tracker;

#[derive(Debug)]
pub struct Namenode {
    data_registry: Arc<DataRegistry>,
    cancel_token: CancellationToken,
    _shutdown_send: UnboundedSender<i8>,
}

impl Namenode {
    pub fn new(cancel_token: CancellationToken, _shutdown_send: UnboundedSender<i8>) -> Self {
        Self {
            data_registry: Arc::new(DataRegistry::new(cancel_token.clone())),
            cancel_token,
            _shutdown_send,
        }
    }

    pub async fn run(&self, addr: SocketAddr) -> CuddlyResult<()> {
        let rpc_service = Server::builder()
            .add_service(HeartbeatServiceServer::new(NamenodeHeartbeatService::new(
                Arc::clone(&self.data_registry),
            )))
            .add_service(NodeServiceServer::new(NamenodeNodeService::new(
                Arc::clone(&self.data_registry),
            )))
            .serve(addr);

        tokio::select! {
            _ = rpc_service => {},

            _ = self.data_registry.run() => {
                info!("DataRegistry Run finished");
            }

            _ = self.cancel_token.cancelled() => {
            info!("Namenode Run cancelled");
            }
        }

        Ok(())
    }
}
