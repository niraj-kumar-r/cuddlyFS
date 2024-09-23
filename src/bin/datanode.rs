use chrono::prelude::{DateTime, Utc};
use local_ip_address::local_ip;
use std::vec;
use tokio::task;
use tokio::time::{interval, Duration};
use tonic::transport::Channel;
use tonic::Response;
use uuid::Uuid;

use cuddlyfs::heartbeat_service_client::HeartbeatServiceClient;
use cuddlyfs::{HeartbeatRequest, HeartbeatResponse};

pub mod cuddlyfs {
    tonic::include_proto!("cuddlyproto");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let node = HeartbeatServiceClient::connect("http://[::1]:50051").await?;

    let mut node_clone = node.clone();
    let handle = task::spawn(async move {
        let mut interval = interval(Duration::from_secs(3)); // Set the interval to 10 seconds
        loop {
            interval.tick().await;
            match send_heartbeat(&mut node_clone).await {
                Ok(_) => println!("Heartbeat sent successfully"),
                Err(e) => eprintln!("Failed to send heartbeat: {:?}", e),
            }
        }
    });

    print!("Main thread is free");

    handle.await.unwrap();

    Ok(())
}

async fn send_heartbeat(
    client: &mut HeartbeatServiceClient<Channel>,
) -> Result<Response<HeartbeatResponse>, Box<dyn std::error::Error>> {
    let ip = local_ip().unwrap();
    let now: DateTime<Utc> = Utc::now();

    let req = tonic::Request::new(HeartbeatRequest {
        registration: Some(cuddlyfs::DatanodeRegistrationProto {
            datanode_id: Some(cuddlyfs::DatanodeIdProto {
                ip_addr: ip.to_string(),
                host_name: "fedoraDatanode.0.0.1".to_string(),
                datanode_uuid: Uuid::new_v4().to_string(),
                xfer_port: 50010,
                info_port: 50075,
                ipc_port: 50020,
                info_secure_port: 50070,
            }),
            storage_info: Some(cuddlyfs::StorageInfoProto {
                layout_version: 1,
                namespace_id: 1,
                cluster_id: 1.to_string(),
                creation_time: now.timestamp() as u64,
            }),
            keys: Some(cuddlyfs::ExportedBlockKeysProto {
                is_block_token_enabled: false,
                key_update_interval: 1,
                token_life_time: 1,
                current_key: Some(cuddlyfs::BlockKeyProto {
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

    let response = client.heartbeat(req).await?;

    Ok(response)
}
