use std::sync::Arc;

use bytes::BytesMut;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};

use crate::{block::Block, utils::calculate_md5_checksum, APP_CONFIG};

use self::cuddlyproto::{Packet, ReadBlockRequest, WriteBlockResponse};

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
        let ReadBlockRequest { block } = request.into_inner();
        let block: Block = block.unwrap().into();
        let block_id = block.id.clone().to_string();

        let blockfile = self
            .datanode_data_registry
            .get_blockfile(&block.filename(), false)
            .await
            .unwrap();
        let mut reader = BufReader::new(blockfile);
        let metadata = reader.get_ref().metadata().await.unwrap();
        let total_size = metadata.len();

        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            let mut sequence_number = 0;
            let mut remaining_to_send = total_size;

            while remaining_to_send > 0 {
                let packet_size = std::cmp::min(remaining_to_send, APP_CONFIG.packet_size);
                let mut buffer = BytesMut::with_capacity(packet_size.clone() as usize);

                match reader.read_buf(&mut buffer).await {
                    Ok(n) => {
                        let buffer = buffer.freeze();

                        let packet = Packet {
                            sequence_number,
                            size: packet_size,
                            checksum: calculate_md5_checksum(&buffer),
                            payload: buffer,
                            is_last: remaining_to_send == packet_size,
                            block_id: block_id.clone(),
                        };

                        if tx.send(Ok(packet)).await.is_err() {
                            break;
                        }

                        remaining_to_send -= n as u64;
                        sequence_number += 1;
                    }
                    Err(err) => {
                        let error = Status::internal(format!("Error reading block: {:?}", err));
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                }
            }

            drop(tx);
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn write_block(
        &self,
        request: tonic::Request<Streaming<Packet>>,
    ) -> Result<tonic::Response<WriteBlockResponse>, tonic::Status> {
        let mut stream = request.into_inner();

        let mut total_written = 0;
        let mut block_file: Option<BufWriter<File>> = None;
        let mut packet_block_id = Option::None;

        while let Some(packet) = stream.message().await? {
            if block_file.is_none() {
                packet_block_id = Some(packet.block_id.clone());
                let file_path = self
                    .datanode_data_registry
                    .get_filepath_for_block_id(&packet.block_id);

                let file = self
                    .datanode_data_registry
                    .get_blockfile(&file_path, true)
                    .await
                    .unwrap();
                block_file = Some(BufWriter::new(file));
            }
            let computed_checksum = calculate_md5_checksum(&packet.payload);
            if computed_checksum != packet.checksum {
                return Err(Status::invalid_argument("Checksum mismatch"));
            }

            if let Some(writer) = &mut block_file {
                writer
                    .write_all(&packet.payload)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                total_written += packet.payload.len();
            }
        }
        let response = WriteBlockResponse {
            status: Some(cuddlyproto::StatusCode {
                success: true,
                code: 0,
                message: format!("Block written successfully, size: {}", total_written),
            }),
        };

        let block = cuddlyproto::Block {
            id: packet_block_id.unwrap(),
            len: total_written as u64,
        };

        if self.received_block_tx.send(block).await.is_err() {
            return Err(Status::internal("Failed to send block to processing"));
        }

        Ok(tonic::Response::new(response))
    }
}
