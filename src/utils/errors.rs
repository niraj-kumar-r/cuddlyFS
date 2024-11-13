use std::fmt::{Display, Formatter};

use config;

#[derive(Debug, PartialEq)]
#[allow(dead_code)]
pub enum CuddlyError {
    IOError(String),
    RPCError(String),
    AddressParseError(String),
    ConfigError(String),
    FSError(String),
    ArgMissingError(String),
    WaitingForReplication(String),
    ProtoError(String),
}

impl Display for CuddlyError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for CuddlyError {}

impl From<std::io::Error> for CuddlyError {
    fn from(error: std::io::Error) -> Self {
        CuddlyError::IOError(error.to_string())
    }
}

impl From<tonic::transport::Error> for CuddlyError {
    fn from(error: tonic::transport::Error) -> Self {
        CuddlyError::RPCError(error.to_string())
    }
}

impl From<std::net::AddrParseError> for CuddlyError {
    fn from(error: std::net::AddrParseError) -> Self {
        CuddlyError::AddressParseError(error.to_string())
    }
}

impl From<config::ConfigError> for CuddlyError {
    fn from(error: config::ConfigError) -> Self {
        CuddlyError::ConfigError(error.to_string())
    }
}

impl From<tonic::Status> for CuddlyError {
    fn from(error: tonic::Status) -> Self {
        CuddlyError::RPCError(error.to_string())
    }
}

impl From<prost::EncodeError> for CuddlyError {
    fn from(error: prost::EncodeError) -> Self {
        CuddlyError::ProtoError(error.to_string())
    }
}

impl From<prost::DecodeError> for CuddlyError {
    fn from(error: prost::DecodeError) -> Self {
        CuddlyError::ProtoError(error.to_string())
    }
}

pub type CuddlyResult<T> = std::result::Result<T, CuddlyError>;
