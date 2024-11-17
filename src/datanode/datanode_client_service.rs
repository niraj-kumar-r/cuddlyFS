use std::sync::Arc;

use tokio::sync::mpsc;

use super::{
    cuddlyproto::{self, client_data_node_service_server::ClientDataNodeService},
    datanode_data_registry::DatanodeDataRegistry,
};

pub struct DatanodeClientService {
    datanode_data_registry: Arc<DatanodeDataRegistry>,
    received_block_tx: mpsc::Sender<cuddlyproto::Block>,
}

impl DatanodeClientService {
    pub fn new(
        datanode_data_registry: Arc<DatanodeDataRegistry>,
        received_block_tx: mpsc::Sender<cuddlyproto::Block>,
    ) -> Self {
        Self {
            datanode_data_registry,
            received_block_tx,
        }
    }
}

impl ClientDataNodeService for DatanodeClientService {}
