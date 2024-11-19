use std::{future::Future, pin::Pin, task::Poll};

use tokio::io::{AsyncRead, BufReader};

use crate::{
    cuddlyproto::{
        client_data_node_service_client::ClientDataNodeServiceClient,
        file_service_client::FileServiceClient, BlockWithLocations, OpenFileRequest, Packet,
        ReadBlockRequest,
    },
    errors::CuddlyResult,
    APP_CONFIG,
};

#[allow(dead_code)]
pub struct CuddlyReader {
    blocks_with_locations: Vec<BlockWithLocations>,
    total_file_size: u64,
    current_block_offset: u64,
    current_block_index: usize,
    current_future: Option<Pin<Box<dyn Future<Output = Result<Packet, std::io::Error>> + Send>>>,
}

impl CuddlyReader {
    pub async fn open(
        namenode_rpc_address: String,
        file_path: impl Into<String>,
    ) -> CuddlyResult<BufReader<Self>> {
        let mut namenode_client =
            match FileServiceClient::connect(namenode_rpc_address.clone()).await {
                Ok(client) => client,
                Err(e) => {
                    return Err(e.into());
                }
            };

        let res = namenode_client
            .open_file(OpenFileRequest {
                auth_token: None,
                file_path: file_path.into(),
            })
            .await?
            .into_inner();

        let mut blocks_with_locations = res.blocks_with_locations;

        let total_file_size = blocks_with_locations
            .iter()
            .fold(0, |acc, b| acc + b.block.as_ref().unwrap().len);

        blocks_with_locations.sort_by(|a, b| {
            a.block
                .as_ref()
                .unwrap()
                .seq
                .cmp(&b.block.as_ref().unwrap().seq)
        });

        Ok(BufReader::with_capacity(
            3 * APP_CONFIG.block_size as usize,
            Self {
                blocks_with_locations,
                total_file_size,
                current_block_offset: 0,
                current_block_index: 0,
                current_future: None,
            },
        ))
    }
}

impl AsyncRead for CuddlyReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let self_mut = self.get_mut();

        if self_mut.current_block_index >= self_mut.blocks_with_locations.len() {
            return Poll::Ready(Ok(()));
        }

        let current_block_with_location =
            &self_mut.blocks_with_locations[self_mut.current_block_index as usize];
        let block = current_block_with_location.block.as_ref().unwrap().clone();

        let data_node_address = current_block_with_location.locations[0].clone();

        if self_mut.current_future.is_none() {
            let fut = async move {
                let mut data_node_client = ClientDataNodeServiceClient::connect(data_node_address)
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                let mut stream = data_node_client
                    .read_block(ReadBlockRequest { block: Some(block) })
                    .await
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
                    .into_inner();

                while let Some(packet) = stream.message().await.map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("Stream error: {e}"))
                })? {
                    return Ok(packet);
                }
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "End of stream",
                ))
            };
            self_mut.current_future = Some(Box::pin(fut));
        }

        let fut = self_mut.current_future.as_mut().unwrap();
        match fut.as_mut().poll(cx) {
            Poll::Ready(Ok(packet)) => {
                buf.put_slice(&packet.payload);
                self_mut.current_block_offset += packet.size;

                if packet.is_last {
                    self_mut.current_block_index += 1;
                    self_mut.current_block_offset = 0;
                    self_mut.current_future = None;
                }

                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e))
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    && e.to_string() == "End of stream" =>
            {
                self_mut.current_block_index += 1;
                self_mut.current_block_offset = 0;
                self_mut.current_future = None;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => {
                self_mut.current_future = None;
                Poll::Ready(Err(e))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
