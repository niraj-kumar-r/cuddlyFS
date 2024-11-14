use std::net::IpAddr;

use uuid::Uuid;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DatanodeInfo {
    pub(crate) ip_address: IpAddr,
    pub(crate) datanode_uuid: Uuid,
    pub(crate) total_capacity: u64,
    pub(crate) used_capacity: u64,
}

#[allow(dead_code)]
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

    pub(crate) fn ip_address(&self) -> &IpAddr {
        &self.ip_address
    }

    pub(crate) fn datanode_uuid(&self) -> &Uuid {
        &self.datanode_uuid
    }

    pub(crate) fn total_capacity(&self) -> u64 {
        self.total_capacity
    }

    pub(crate) fn used_capacity(&self) -> u64 {
        self.used_capacity
    }

    pub(crate) fn free_capacity(&self) -> u64 {
        self.total_capacity - self.used_capacity
    }
}
