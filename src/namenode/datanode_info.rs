use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cuddlyproto;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

impl std::fmt::Display for DatanodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DatanodeInfo {{ ip_address: {}, datanode_uuid: {}, total_capacity: {}, used_capacity: {} }}",
            self.ip_address, self.datanode_uuid, self.total_capacity, self.used_capacity
        )
    }
}

impl From<cuddlyproto::DatanodeInfo> for DatanodeInfo {
    fn from(value: cuddlyproto::DatanodeInfo) -> Self {
        let cuddlyproto::DatanodeInfo {
            ip_address,
            datanode_uuid,
            total_capacity,
            used_capacity,
        } = value;
        Self {
            ip_address: ip_address.parse().unwrap(),
            datanode_uuid: Uuid::parse_str(&datanode_uuid).unwrap(),
            total_capacity,
            used_capacity,
        }
    }
}

impl From<DatanodeInfo> for cuddlyproto::DatanodeInfo {
    fn from(value: DatanodeInfo) -> Self {
        let DatanodeInfo {
            ip_address,
            datanode_uuid,
            total_capacity,
            used_capacity,
        } = value;
        let ip_address = ip_address.to_string();
        let datanode_uuid = datanode_uuid.to_string();
        Self {
            ip_address,
            datanode_uuid,
            total_capacity,
            used_capacity,
        }
    }
}
