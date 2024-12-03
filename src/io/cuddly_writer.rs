use prost::Message;

use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufStream, SeekFrom};
use tokio::net::TcpStream;
use tokio::time;

use tonic::transport::Channel;

use crate::cuddlyproto::file_service_client::FileServiceClient;
use crate::errors::{CuddlyError, CuddlyResult};
use crate::utils::parse_message;
use crate::{cuddlyproto, APP_CONFIG};

use self::cuddlyproto::WriteBlockResponse;

async fn new_backup_file() -> CuddlyResult<BufStream<File>> {
    let backup_dir = std::env::temp_dir();
    let filename = format!("tmp_{}", rand::random::<u64>());
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(backup_dir.join(filename))
        .await?;
    Ok(BufStream::new(file))
}

pub struct DfsWriter {
    namenode_client: FileServiceClient<Channel>,
    block_size: u64,
    packet_size: u64,
    path: String,
    file_started: bool,
    bytes_written_to_block: u64,
    backup_buffer: BufStream<File>,
}

impl DfsWriter {
    pub async fn create(path: impl Into<String>, namenode_rpc_address: &str) -> CuddlyResult<Self> {
        let client = match FileServiceClient::connect(String::from(namenode_rpc_address)).await {
            Ok(client) => client,
            Err(err) => {
                return Err(CuddlyError::RPCError(format!(
                    "Could not connect to namenode: {}",
                    err
                )))
            }
        };

        Ok(Self {
            namenode_client: client,
            block_size: APP_CONFIG.block_size,
            packet_size: APP_CONFIG.packet_size,
            path: path.into(),
            file_started: false,
            bytes_written_to_block: 0,
            backup_buffer: new_backup_file().await?,
        })
    }

    pub async fn write(&mut self, buf: &[u8]) -> CuddlyResult<()> {
        let start_pos = if self.bytes_written_to_block + buf.len() as u64 >= self.block_size {
            let bytes_to_fill_block = self.block_size - self.bytes_written_to_block;
            let buf = &buf[..(bytes_to_fill_block as usize)];
            self.backup_buffer.write_all(&buf).await?;
            self.bytes_written_to_block += bytes_to_fill_block;
            self.flush().await?;

            bytes_to_fill_block
        } else {
            0
        };

        self.backup_buffer
            .write_all(&buf[(start_pos as usize)..])
            .await?;
        self.bytes_written_to_block += buf.len() as u64 - start_pos;
        Ok(())
    }

    pub async fn flush(&mut self) -> CuddlyResult<()> {
        self.backup_buffer.flush().await?;

        if self.bytes_written_to_block >= self.block_size {
            self.write_block().await?;
        }

        Ok(())
    }

    pub async fn shutdown(&mut self) -> CuddlyResult<()> {
        if self.bytes_written_to_block > 0 {
            self.backup_buffer.flush().await?;
            self.write_block().await?;
        }

        let max_retries = 10;
        let mut sleep_time = time::Duration::from_millis(500);
        for _ in 0..max_retries {
            let response = self
                .namenode_client
                .finish_file_create(cuddlyproto::CreateFileRequest {
                    file_path: self.path.clone(),
                })
                .await;
            match response {
                Ok(_) => return Ok(()),
                Err(status) => {
                    // we set code to unavailable if not all blocks have been
                    // replicated yet. Wait for some time, maybe the block will be
                    // eventually replicated
                    if status.code() == tonic::Code::Unavailable {
                        time::sleep(sleep_time).await;
                        sleep_time *= 2;
                        continue;
                    } else {
                        return Err(status.into());
                    }
                }
            }
        }

        Err(CuddlyError::FSError(format!(
            "Failed to save file after {} retries.",
            max_retries
        )))
    }

    async fn next_block(
        &mut self,
    ) -> CuddlyResult<(cuddlyproto::Block, Vec<cuddlyproto::DatanodeInfo>)> {
        let cuddlyproto::BlockWithTargets { block, targets } = if self.file_started {
            self.following_block().await?
        } else {
            self.namenode_client
                .start_file_create(cuddlyproto::CreateFileRequest {
                    file_path: self.path.clone(),
                })
                .await?
                .into_inner()
                .block_with_targets
                .unwrap()
        };
        self.file_started = true;

        Ok((block.unwrap(), targets))
    }

    async fn following_block(&mut self) -> CuddlyResult<cuddlyproto::BlockWithTargets> {
        let max_retries = 10;
        let mut sleep_time = time::Duration::from_millis(500);
        for _ in 0..max_retries {
            let response = self
                .namenode_client
                .add_block(cuddlyproto::AddBlockRequest {
                    path: self.path.clone(),
                })
                .await;

            match response {
                Ok(message) => return Ok(message.into_inner().block_with_targets.unwrap()),
                Err(status) => {
                    // we set code to unavailable if not all blocks have been
                    // replicated yet. Wait for some time, maybe the block will be
                    // eventually replicated
                    if status.code() == tonic::Code::Unavailable {
                        time::sleep(sleep_time).await;
                        sleep_time *= 2;
                        continue;
                    } else {
                        return Err(status.into());
                    }
                }
            }
        }
        Err(CuddlyError::FSError(format!(
            "Failed to add new block after {} retries.",
            max_retries
        )))
    }

    async fn write_block(&mut self) -> CuddlyResult<()> {
        let (block, targets) = self.next_block().await?;

        let mut datanode = TcpStream::connect(&targets[0].ip_address).await?;
        let mut buffer = vec![];
        let op = cuddlyproto::Operation {
            op: cuddlyproto::operation::OpCode::WriteBlock as i32,
        };
        op.encode_length_delimited(&mut buffer)?;
        datanode.write_all(&buffer).await?;
        buffer.clear();

        let write_op = cuddlyproto::WriteBlockOperation {
            block: Some(block),
            targets: targets.iter().map(|info| info.ip_address.clone()).collect(),
        };
        write_op.encode_length_delimited(&mut buffer)?;
        datanode.write_all(&buffer).await?;
        buffer.clear();

        let mut remaining_to_send = self.bytes_written_to_block;
        self.backup_buffer
            .get_mut()
            .seek(SeekFrom::Start(0u64))
            .await?;
        while remaining_to_send > 0 {
            let packet_size = std::cmp::min(remaining_to_send, self.packet_size);
            remaining_to_send -= packet_size;
            let packet = cuddlyproto::Packet {
                size: packet_size,
                last: remaining_to_send == 0,
            };
            packet.encode_length_delimited(&mut buffer)?;
            datanode.write_all(&buffer).await?;
            buffer.clear();
            buffer.resize_with(packet_size as usize, u8::default);
            self.backup_buffer.read_exact(&mut buffer).await?;
            datanode.write_all(&buffer).await?;
            buffer.clear();
        }
        datanode.flush().await?;

        let cuddlyproto::WriteBlockResponse { success } =
            parse_message::<WriteBlockResponse>(&mut datanode).await?;

        if !success {
            return Err(CuddlyError::FSError(
                "Replicating block was not successful".to_owned(),
            ));
        }

        self.backup_buffer
            .get_mut()
            .seek(SeekFrom::Start(0u64))
            .await?;
        self.bytes_written_to_block = 0;

        Ok(())
    }
}
