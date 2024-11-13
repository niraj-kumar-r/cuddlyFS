use std::fmt::{Display, Formatter};

use config;

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum CudddlyError {
    IOError(String),
    RPCError(String),
    AddressParseError(String),
    ConfigError(String),
    FSError(String),
    ArgMissingError(String),
    WaitingForReplication(String),
    ProtoError(String),
}

impl Display for CudddlyError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for CudddlyError {}

impl From<std::io::Error> for CudddlyError {
    fn from(error: std::io::Error) -> Self {
        CudddlyError::IOError(error.to_string())
    }
}

impl From<tonic::transport::Error> for CudddlyError {
    fn from(error: tonic::transport::Error) -> Self {
        CudddlyError::RPCError(error.to_string())
    }
}

impl From<std::net::AddrParseError> for CudddlyError {
    fn from(error: std::net::AddrParseError) -> Self {
        CudddlyError::AddressParseError(error.to_string())
    }
}

impl From<config::ConfigError> for CudddlyError {
    fn from(error: config::ConfigError) -> Self {
        CudddlyError::ConfigError(error.to_string())
    }
}

impl From<tonic::Status> for CudddlyError {
    fn from(error: tonic::Status) -> Self {
        CudddlyError::RPCError(error.to_string())
    }
}

impl From<prost::EncodeError> for CudddlyError {
    fn from(error: prost::EncodeError) -> Self {
        CudddlyError::ProtoError(error.to_string())
    }
}

impl From<prost::DecodeError> for CudddlyError {
    fn from(error: prost::DecodeError) -> Self {
        CudddlyError::ProtoError(error.to_string())
    }
}

pub type CuddlyResult<T> = std::result::Result<T, CudddlyError>;
