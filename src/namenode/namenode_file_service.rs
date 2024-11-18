use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::{
    cuddlyproto::{
        self, file_service_server::FileService, AbortBlockWriteRequest, AddBlockRequest,
        AddBlockResponse, CreateDirectoryRequest, CreateDirectoryResponse, CreateFileRequest,
        CreateFileResponse, ListDirectoryRequest, ListDirectoryResponse, OpenFileRequest,
        OpenFileResponse, ReportDatanodesRequest, ReportDatanodesResponse, StatusCode,
    },
    errors::CuddlyError,
};

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

    async fn open_file(
        &self,
        request: Request<OpenFileRequest>,
    ) -> Result<Response<OpenFileResponse>, Status> {
        let request = request.into_inner();
        let blocks_with_locations = self.data_registry.open_file(&request.file_path);

        match blocks_with_locations {
            Ok(blocks_with_locations) => {
                let res = blocks_with_locations
                    .into_iter()
                    .map(|(block, locations)| cuddlyproto::BlockWithLocations {
                        block: Some(block.into()),
                        locations: locations
                            .into_iter()
                            .map(|location| location.ip_address.to_string())
                            .collect(),
                    })
                    .collect();

                Ok(Response::new(OpenFileResponse {
                    blocks_with_locations: res,
                    status: Some(cuddlyproto::StatusCode {
                        success: true,
                        code: cuddlyproto::StatusEnum::Ok as i32,
                        message: "File opened successfully".to_string(),
                    }),
                }))
            }
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn start_file_create(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let request = request.into_inner();
        let res = self.data_registry.start_file_create(&request.file_path);

        match res {
            Ok(Some((block, targets))) => Ok(Response::new(CreateFileResponse {
                block_with_targets: Some(cuddlyproto::BlockWithTargets {
                    block: Some(block.into()),
                    targets: targets.into_iter().map(|target| target.into()).collect(),
                }),
            })),
            Ok(None) => Err(Status::failed_precondition(
                "Cannot create file, not enough avaialable datanodes with free space",
            )),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn finish_file_create(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<StatusCode>, Status> {
        let request = request.into_inner();
        match self
            .data_registry
            .finish_file_create(&request.file_path)
            .await
        {
            Ok(()) => Ok(Response::new(StatusCode {
                success: true,
                code: cuddlyproto::StatusEnum::Ok as i32,
                message: "File created successfully".to_string(),
            })),
            Err(CuddlyError::WaitingForReplication(err)) => Err(Status::unavailable(err)),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn abort_file_create(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<StatusCode>, Status> {
        let request = request.into_inner();
        match self.data_registry.abort_file_create(&request.file_path) {
            Ok(()) => Ok(Response::new(StatusCode {
                success: true,
                code: cuddlyproto::StatusEnum::Ok as i32,
                message: "File creation aborted".to_string(),
            })),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn add_block(
        &self,
        request: Request<AddBlockRequest>,
    ) -> Result<Response<AddBlockResponse>, Status> {
        let request = request.into_inner();

        let res = self.data_registry.start_another_block(&request.path);

        match res {
            Ok(Some((block, targets))) => {
                let block: cuddlyproto::Block = block.into();
                let targets: Vec<cuddlyproto::DatanodeInfo> =
                    targets.into_iter().map(|info| info.into()).collect();

                Ok(Response::new(AddBlockResponse {
                    block_with_targets: Some(cuddlyproto::BlockWithTargets {
                        block: Some(block),
                        targets,
                    }),
                }))
            }
            Ok(None) => Err(Status::failed_precondition(
                "Unable to create another block: insufficient available datanodes with free space",
            )),
            Err(CuddlyError::WaitingForReplication(err)) => Err(Status::unavailable(err)),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }

    async fn abort_block_write(
        &self,
        request: Request<AbortBlockWriteRequest>,
    ) -> Result<Response<StatusCode>, Status> {
        let request = request.into_inner();
        let block = request.block.unwrap().into();
        match self.data_registry.abort_block(&request.path, &block) {
            Ok(()) => Ok(Response::new(StatusCode {
                success: true,
                code: cuddlyproto::StatusEnum::Ok as i32,
                message: "Block write aborted".to_string(),
            })),
            Err(err) => Err(Status::invalid_argument(err.to_string())),
        }
    }
}
