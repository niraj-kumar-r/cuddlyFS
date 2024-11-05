use std::net::IpAddr;

use uuid::Uuid;

pub(crate) struct DatanodeInfo {
    ip_address: IpAddr,
    datanode_uuid: Uuid,
    total_capacity: u64,
    used_capacity: u64,
}

impl DatanodeInfo {
    pub(crate) fn new(
        ip_address: impl Into<IpAddr>,
        datanode_uuid: impl Into<Uuid>,
        total_capacity: u64,
        used_capacity: u64,
    ) -> Self {
        Self {
            ip_address: ip_address.into(),
            datanode_uuid: datanode_uuid.into(),
            total_capacity,
            used_capacity,
        }
    }
}
