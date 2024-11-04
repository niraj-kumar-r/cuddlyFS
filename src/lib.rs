pub(crate) mod cuddlyproto {
    tonic::include_proto!("cuddlyproto");
}
pub(crate) mod config;
pub use config::APP_CONFIG;
pub(crate) mod block;
pub mod datanode;
pub mod fs_client;
pub mod namenode;
