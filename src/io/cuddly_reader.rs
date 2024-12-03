use std::io::Read;

use prost::Message;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::{io::BufStream, net::TcpStream};

use crate::utils::parse_message;
use crate::{
    cuddlyproto::{
        self, file_service_client::FileServiceClient, BlockWithLocations, OpenFileRequest,
    },
    errors::CuddlyResult,
};

#[allow(dead_code)]
pub struct CuddlyReader {
    blocks_with_locations: Vec<BlockWithLocations>,
    block_index: usize,
    total_file_size: u64,
    current_file_pos: u64,
    current_reader: BufStream<TcpStream>,
    current_block_size: u64,
    current_block_pos: u64,
    current_packet_size: u64,
    current_packet_pos: u64,
    buffer: Vec<u8>,
    // current_future: Option<Pin<Box<dyn Future<Output = Result<Packet, std::io::Error>> + Send>>>,
}

impl CuddlyReader {
    pub async fn open(
        namenode_rpc_address: String,
        file_path: impl Into<String>,
    ) -> CuddlyResult<Self> {
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

        let current_block = blocks_with_locations[0].block.as_ref().unwrap().clone();
        let mut current_reader =
            BufStream::new(TcpStream::connect(&blocks_with_locations[0].locations[0]).await?);

        let mut buffer = vec![];
        let op = cuddlyproto::Operation {
            op: cuddlyproto::operation::OpCode::ReadBlock as i32,
        };
        op.encode_length_delimited(&mut buffer)?;
        current_reader.write_all(&buffer).await?;
        buffer.clear();

        let read_op = cuddlyproto::ReadBlockOperation {
            block: Some(current_block.clone()),
        };
        read_op.encode_length_delimited(&mut buffer)?;
        current_reader.write_all(&buffer).await?;
        current_reader.flush().await?;

        Ok(Self {
            blocks_with_locations,
            block_index: 0,
            total_file_size,
            current_file_pos: 0,
            current_block_size: current_block.len,
            current_packet_size: 0,
            current_block_pos: 0,
            current_packet_pos: 0,
            current_reader,
            buffer: Vec::new(),
        })
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> CuddlyResult<usize> {
        if self.current_file_pos == self.total_file_size {
            return Ok(0);
        } else if self.current_block_pos == self.current_block_size {
            self.next_block().await?;
        } else if self.current_packet_pos == self.current_packet_size {
            self.next_packet().await?;
        }

        let buffer_start_pos = self.current_packet_pos as usize;
        let bytes_read = Read::read(&mut (&self.buffer[buffer_start_pos..]), buf)? as u64;
        self.current_file_pos += bytes_read;
        self.current_block_pos += bytes_read;
        self.current_packet_pos += bytes_read;

        Ok(bytes_read as usize)
    }

    pub async fn next_block(&mut self) -> CuddlyResult<()> {
        self.block_index += 1;
        let current_block = self.blocks_with_locations[self.block_index]
            .block
            .as_ref()
            .unwrap();

        self.current_block_size = current_block.len;
        self.current_block_pos = 0;

        let mut current_reader = BufStream::new(
            TcpStream::connect(&self.blocks_with_locations[self.block_index].locations[0]).await?,
        );
        self.buffer.clear();

        let op = cuddlyproto::Operation {
            op: cuddlyproto::operation::OpCode::ReadBlock as i32,
        };
        op.encode_length_delimited(&mut self.buffer)?;
        current_reader.write_all(&self.buffer).await?;
        self.buffer.clear();

        let read_op = cuddlyproto::ReadBlockOperation {
            block: Some(current_block.clone()),
        };
        read_op.encode_length_delimited(&mut self.buffer)?;

        current_reader.write_all(&self.buffer).await?;
        current_reader.flush().await?;

        self.current_reader = current_reader;
        self.next_packet().await?;

        Ok(())
    }

    pub async fn next_packet(&mut self) -> CuddlyResult<()> {
        let cuddlyproto::Packet { size, last: _ } =
            parse_message::<cuddlyproto::Packet>(&mut self.current_reader).await?;
        self.current_packet_pos = 0;
        self.current_packet_size = size;
        self.buffer.clear();
        self.buffer.resize_with(size as usize, u8::default);
        self.current_reader.read_exact(&mut self.buffer).await?;
        Ok(())
    }
}

// impl AsyncRead for CuddlyReader {
//     fn poll_read(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//         buf: &mut tokio::io::ReadBuf<'_>,
//     ) -> Poll<std::io::Result<()>> {
//         let self_mut = self.get_mut();

//         if self_mut.current_block_index >= self_mut.blocks_with_locations.len() {
//             return Poll::Ready(Ok(()));
//         }

//         let current_block_with_location =
//             &self_mut.blocks_with_locations[self_mut.current_block_index as usize];
//         let block = current_block_with_location.block.as_ref().unwrap().clone();

//         let data_node_address = current_block_with_location.locations[0].clone();

//         if self_mut.current_future.is_none() {
//             let fut = async move {
//                 let mut data_node_client = ClientDataNodeServiceClient::connect(data_node_address)
//                     .await
//                     .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

//                 let mut stream = data_node_client
//                     .read_block(ReadBlockRequest { block: Some(block) })
//                     .await
//                     .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?
//                     .into_inner();

//                 while let Some(packet) = stream.message().await.map_err(|e| {
//                     std::io::Error::new(std::io::ErrorKind::Other, format!("Stream error: {e}"))
//                 })? {
//                     return Ok(packet);
//                 }
//                 Err(std::io::Error::new(
//                     std::io::ErrorKind::UnexpectedEof,
//                     "End of stream",
//                 ))
//             };
//             self_mut.current_future = Some(Box::pin(fut));
//         }

//         let fut = self_mut.current_future.as_mut().unwrap();
//         match fut.as_mut().poll(cx) {
//             Poll::Ready(Ok(packet)) => {
//                 buf.put_slice(&packet.payload);
//                 self_mut.current_block_offset += packet.size;

//                 if packet.is_last {
//                     self_mut.current_block_index += 1;
//                     self_mut.current_block_offset = 0;
//                     self_mut.current_future = None;
//                 }

//                 Poll::Ready(Ok(()))
//             }
//             Poll::Ready(Err(e))
//                 if e.kind() == std::io::ErrorKind::UnexpectedEof
//                     && e.to_string() == "End of stream" =>
//             {
//                 self_mut.current_block_index += 1;
//                 self_mut.current_block_offset = 0;
//                 self_mut.current_future = None;
//                 Poll::Ready(Ok(()))
//             }
//             Poll::Ready(Err(e)) => {
//                 self_mut.current_future = None;
//                 Poll::Ready(Err(e))
//             }
//             Poll::Pending => Poll::Pending,
//         }
//     }
// }
