use std::sync::Arc;

use tonic::Request;

use crate::cuddlyproto::{
    directory_service_server::DirectoryService, CreateDirectoryRequest, CreateDirectoryResponse,
    ListDirectoryRequest, ListDirectoryResponse, ReportDatanodesRequest, ReportDatanodesResponse,
    StatusCode,
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
    async fn report_datanodes(
        &self,
        _request: Request<ReportDatanodesRequest>,
    ) -> Result<tonic::Response<ReportDatanodesResponse>, tonic::Status> {
        match self.data_registry.report_datanodes() {
            Ok(dat) => Ok(tonic::Response::new(ReportDatanodesResponse {
                datanodes: dat.iter().map(|d| d.clone().into()).collect(),
            })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequest>,
    ) -> Result<tonic::Response<CreateDirectoryResponse>, tonic::Status> {
        let request_data = request.into_inner();

        match self
            .data_registry
            .make_dir(&request_data.directory_path)
            .await
        {
            Ok(_) => Ok(tonic::Response::new(CreateDirectoryResponse {
                status: Some(StatusCode {
                    success: true,
                    code: 0,
                    message: "Success".to_string(),
                }),
            })),
            Err(e) => Ok(tonic::Response::new(CreateDirectoryResponse {
                status: Some(StatusCode {
                    success: false,
                    code: 1,
                    message: e.to_string(),
                }),
            })),
        }
    }

    async fn list_directory(
        &self,
        request: Request<ListDirectoryRequest>,
    ) -> Result<tonic::Response<ListDirectoryResponse>, tonic::Status> {
        let request_data = request.into_inner();

        match self.data_registry.list(&request_data.directory_path) {
            Ok(files) => Ok(tonic::Response::new(ListDirectoryResponse {
                entries: files,
                status: Some(StatusCode {
                    success: true,
                    code: 0,
                    message: "Success".to_string(),
                }),
            })),
            Err(e) => Ok(tonic::Response::new(ListDirectoryResponse {
                entries: vec![],
                status: Some(StatusCode {
                    success: false,
                    code: 1,
                    message: e.to_string(),
                }),
            })),
        }
    }
}
