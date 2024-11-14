use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cuddlyproto::{
    self, file_service_server::FileService, OpenFileRequest, OpenFileResponse,
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
                            .map(|location| location.to_string())
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
}
