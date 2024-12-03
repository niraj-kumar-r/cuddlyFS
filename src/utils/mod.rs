use errors::{CuddlyError, CuddlyResult};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};

pub mod errors;
pub(crate) mod key_to_data_and_id_map;

pub(crate) fn calculate_md5_checksum<T: AsRef<[u8]>>(data: &T) -> String {
    let digest = md5::compute(data.as_ref());
    format!("{:x}", digest)
}

pub async fn parse_message<T: Message + Default>(
    reader: &mut (impl AsyncRead + Unpin),
) -> CuddlyResult<T> {
    let (size, _) = get_message_size(reader).await?;

    let mut buffer = vec![0u8; size as usize];
    reader.read_exact(buffer.as_mut()).await?;

    let message = T::decode(buffer.as_ref())?;
    Ok(message)
}

async fn get_message_size(reader: &mut (impl AsyncRead + Unpin)) -> CuddlyResult<(u64, u8)> {
    let mut result = 0;
    let mut shift = 0;
    for bytes_read in 1..=10 {
        let tmp = reader.read_u8().await?;
        result |= tmp as u64 & 0x7f << shift;
        if tmp < 0x80 {
            return Ok((result, bytes_read));
        }
        shift += 7;
    }

    Err(CuddlyError::ProtoError("invalid varint".to_owned()))
}
