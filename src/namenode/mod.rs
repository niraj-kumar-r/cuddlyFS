use log::info;
use namenode_data_registry::DataRegistry;
use namenode_file_service::NamenodeFileService;
use namenode_node_service::NamenodeNodeService;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::{
    cuddlyproto::{file_service_server::FileServiceServer, node_service_server::NodeServiceServer},
    errors::CuddlyResult,
};

mod datanode_info;
mod namenode_data_registry;
mod namenode_file_service;
mod namenode_node_service;
mod namenode_operation_logger;
mod namenode_progress_tracker;
mod namenode_state;

#[derive(Debug)]
pub struct Namenode {
    data_registry: Arc<DataRegistry>,
    cancel_token: CancellationToken,
    _shutdown_send: UnboundedSender<i8>,
}

impl Namenode {
    pub fn new(
        cancel_token: CancellationToken,
        _shutdown_send: UnboundedSender<i8>,
    ) -> CuddlyResult<Self> {
        Ok(Self {
            data_registry: Arc::new(DataRegistry::new(cancel_token.clone())?),
            cancel_token,
            _shutdown_send,
        })
    }

    pub async fn run(&self, addr: SocketAddr) -> CuddlyResult<()> {
        let rpc_service = Server::builder()
            .add_service(NodeServiceServer::new(NamenodeNodeService::new(
                Arc::clone(&self.data_registry),
            )))
            .add_service(FileServiceServer::new(NamenodeFileService::new(
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
