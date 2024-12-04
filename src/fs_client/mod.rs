use log::info;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};

use tonic::transport::Channel;
use tracing::error;

use crate::cuddlyproto;
use crate::cuddlyproto::file_service_client::FileServiceClient;
use crate::errors::{CuddlyError, CuddlyResult};
use crate::io::cuddly_reader::CuddlyReader;
use crate::io::cuddly_writer::CuddlyWriter;

pub struct CuddlyClient {
    namenode_rpc_address: String,
    namenode_client: FileServiceClient<Channel>,
}

impl CuddlyClient {
    pub async fn new(_namenode_rpc_address: String) -> CuddlyResult<Self> {
        let addr = format!("{}:{}", "http://localhost", "50051");
        info!("Trying to connect to namenode at {}", addr);
        match FileServiceClient::connect(addr.clone()).await {
            Ok(client) => {
                info!("Connected to namenode at {}", addr);
                Ok(Self {
                    namenode_rpc_address: addr,
                    namenode_client: client,
                })
            }
            Err(err) => {
                error!("Could not connect to namenode: {}", err);
                return Err(CuddlyError::RPCError(format!(
                    "Could not connect to namenode: {}",
                    err
                )));
            }
        }
    }

    pub async fn nodes_report(&self) -> CuddlyResult<Vec<cuddlyproto::DatanodeInfo>> {
        let mut client = self.namenode_client.clone();
        let response = client
            .report_datanodes(cuddlyproto::ReportDatanodesRequest { status: None })
            .await?;
        let cuddlyproto::ReportDatanodesResponse { datanodes } = response.into_inner();
        Ok(datanodes)
    }

    pub async fn mkdir(&self, path: impl Into<String>) -> CuddlyResult<()> {
        let mut client = self.namenode_client.clone();
        client
            .create_directory(cuddlyproto::CreateDirectoryRequest {
                auth_token: None,
                directory_path: path.into(),
            })
            .await?;
        Ok(())
    }

    pub async fn ls(&self, path: impl Into<String>) -> CuddlyResult<Vec<String>> {
        let mut client = self.namenode_client.clone();
        let response = client
            .list_directory(cuddlyproto::ListDirectoryRequest {
                auth_token: None,
                directory_path: path.into(),
            })
            .await?;
        let cuddlyproto::ListDirectoryResponse { entries, .. } = response.into_inner();
        Ok(entries)
    }

    pub async fn put(&self, src: &str, dst: impl Into<String>) -> CuddlyResult<()> {
        info!("Uploading file from {}", src);
        let mut reader = BufReader::new(File::open(src).await?);
        let mut writer = CuddlyWriter::create(dst, &self.namenode_rpc_address).await?;

        let mut buf = vec![0; 128];
        loop {
            let read = reader.read(&mut buf).await?;
            if read == 0 {
                break;
            }
            writer.write(&buf[..read]).await?;
        }
        writer.flush().await?;
        writer.shutdown().await?;

        Ok(())
    }

    pub async fn get(&self, src: &str, dst: &str) -> CuddlyResult<()> {
        let mut reader = CuddlyReader::open(self.namenode_rpc_address.clone(), src).await?;
        let mut writer = BufWriter::new(File::create(dst).await?);

        let mut buf = vec![0; 128];
        loop {
            let read = reader.read(&mut buf).await?;
            if read == 0 {
                break;
            }
            writer.write_all(&buf[..read]).await?;
        }
        writer.flush().await?;
        writer.shutdown().await?;

        Ok(())
    }
}
