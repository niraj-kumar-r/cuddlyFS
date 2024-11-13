use std::sync::Arc;

use tonic::Request;

use crate::cuddlyproto::{
    directory_service_server::DirectoryService, CreateDirectoryRequest, CreateDirectoryResponse,
    ListDirectoryRequest, ListDirectoryResponse, StatusCode,
};

use super::namenode_data_registry::DataRegistry;

pub struct NamenodeDirectoryService {
    data_registry: Arc<DataRegistry>,
}

impl NamenodeDirectoryService {
    pub fn new(data_registry: Arc<DataRegistry>) -> Self {
        Self { data_registry }
    }
}

#[tonic::async_trait]
impl DirectoryService for NamenodeDirectoryService {
    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequest>,
    ) -> Result<tonic::Response<CreateDirectoryResponse>, tonic::Status> {
        // let request_data = request.into_inner();

        Ok(tonic::Response::new(CreateDirectoryResponse {
            status: Some(StatusCode {
                success: false,
                code: 0,
                message: "No Success".to_string(),
            }),
        }))
    }

    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<tonic::Response<ListDirectoryResponse>, tonic::Status> {
        // let request_data = request.into_inner();

        Ok(tonic::Response::new(ListDirectoryResponse {
            entries: vec![],
            status: Some(StatusCode {
                success: false,
                code: 0,
                message: "No Success".to_string(),
            }),
        }))
    }
}
