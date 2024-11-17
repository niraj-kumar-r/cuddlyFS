use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use self::cuddlyproto::{Packet, ReadBlockRequest};

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

#[tonic::async_trait]
impl ClientDataNodeService for DatanodeClientService {
    type ReadBlockStream = ReceiverStream<Result<Packet, Status>>;

    async fn read_block(
        &self,
        request: tonic::Request<cuddlyproto::ReadBlockRequest>,
    ) -> Result<tonic::Response<Self::ReadBlockStream>, tonic::Status> {
        let request_data = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
