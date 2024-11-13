use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cuddlyproto::{file_service_server::FileService, OpenFileRequest, OpenFileResponse};

use super::namenode_data_registry::DataRegistry;

pub struct NamenodeFileService {
    data_registry: Arc<DataRegistry>,
}

impl NamenodeFileService {
    pub fn new(data_registry: Arc<DataRegistry>) -> Self {
        Self { data_registry }
    }
}

#[tonic::async_trait]
impl FileService for NamenodeFileService {
    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        Err(Status::invalid_argument("unimplemented"))
    }
}
