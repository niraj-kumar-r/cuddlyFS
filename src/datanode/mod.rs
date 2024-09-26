use crate::cuddlyproto;

use chrono::Utc;
use local_ip_address::local_ip;
use tonic::transport::Channel;
use uuid::Uuid;

#[derive(Clone, Debug, Default)]
pub struct Datanode {
    pub datanode_id: cuddlyproto::DatanodeIdProto,
}

impl Datanode {
    pub fn new() -> Self {
        Datanode {
            datanode_id: cuddlyproto::DatanodeIdProto {
                ip_addr: local_ip().unwrap().to_string(),
                host_name: "fedoraDatanode.0.0.1".to_string(),
                datanode_uuid: Uuid::new_v4().to_string(),
                xfer_port: 50010,
                info_port: 50075,
                ipc_port: 50020,
                info_secure_port: 50070,
            },
        }
    }

    async fn get_heartbeat_client(
        uri: String,
    ) -> Result<
        cuddlyproto::heartbeat_service_client::HeartbeatServiceClient<Channel>,
        Box<dyn std::error::Error>,
    > {
        let client =
            cuddlyproto::heartbeat_service_client::HeartbeatServiceClient::connect(uri).await?;
        Ok(client)
    }

    pub async fn heartbeat(
        &self,
    ) -> Result<tonic::Response<cuddlyproto::HeartbeatResponse>, Box<dyn std::error::Error>> {
        let req = tonic::Request::new(cuddlyproto::HeartbeatRequest {
            registration: Some(cuddlyproto::DatanodeRegistrationProto {
                datanode_id: Some(cuddlyproto::DatanodeIdProto {
                    ip_addr: local_ip().unwrap().to_string(),
                    host_name: "fedoraDatanode.0.0.1".to_string(),
                    datanode_uuid: Uuid::new_v4().to_string(),
                    xfer_port: 50010,
                    info_port: 50075,
                    ipc_port: 50020,
                    info_secure_port: 50070,
                }),
                storage_info: Some(cuddlyproto::StorageInfoProto {
                    layout_version: 1,
                    namespace_id: 1,
                    cluster_id: 1.to_string(),
                    creation_time: Utc::now().timestamp() as u64,
                }),
                keys: Some(cuddlyproto::ExportedBlockKeysProto {
                    is_block_token_enabled: false,
                    key_update_interval: 1,
                    token_life_time: 1,
                    current_key: Some(cuddlyproto::BlockKeyProto {
                        key_id: 1,
                        expiry_date: 3000,
                        key_bytes: Some("my_secret_key".into()),
                    }),
                    all_keys: vec![],
                }),
                software_version: "0.1.0".to_string(),
            }),
            reports: vec![],
        });

        let mut client = Self::get_heartbeat_client("http://[::1]:50051".to_string()).await?;

        let response = client.heartbeat(req).await.unwrap();
        Ok(response)
    }
}
