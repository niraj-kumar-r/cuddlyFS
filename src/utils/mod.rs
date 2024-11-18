pub mod errors;
pub(crate) mod key_to_data_and_id_map;

pub(crate) fn calculate_md5_checksum<T: AsRef<[u8]>>(data: &T) -> String {
    let digest = md5::compute(data.as_ref());
    format!("{:x}", digest)
}
