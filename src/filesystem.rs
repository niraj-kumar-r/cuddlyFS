pub trait FileSystem {
    fn read(&self, file_path: &str) -> Vec<u8>;
    fn write(&mut self, file_path: &str, data: &[u8]) -> Result<(), String>;
    fn delete(&mut self, file_path: &str) -> Result<(), String>;
    fn list(&self, directory: &str) -> Vec<String>;
}
