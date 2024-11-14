use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cuddlyproto::{
    self, file_service_server::FileService, CreateFileRequest, CreateFileResponse, OpenFileRequest,
    OpenFileResponse,
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
                    blocks: res,
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
}
