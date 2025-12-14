use error_stack::Report;
use log::error;
use std::time::SystemTime;

pub struct ReqwestSource {
    client: reqwest::blocking::Client,
}

impl ReqwestSource {
    pub fn new() -> ReqwestSource {
        ReqwestSource {
            client: reqwest::blocking::Client::new(),
        }
    }
    pub fn get_tile(&self, x: i32, y: i32, z: i32) -> Result<Vec<u8>, Report<reqwest::Error>> {
        let t1 = SystemTime::now();
        // TODO Configurable URL
        let response = self.client.get(format!(
            "http://ec2-54-252-214-137.ap-southeast-2.compute.amazonaws.com:3000/tile/{x}/{y}/{z}"
        )).send();
        let td = SystemTime::now();
        let bytes = response.and_then(|response| response.bytes())?
        .to_vec();
        let t2 = SystemTime::now();
        error!(
            "get_tile, x = {}, y = {}, z = {}, total_time = {:?}, download_time = {:?}, len = {}",
            x,
            y,
            z,
            t2.duration_since(t1),
            t2.duration_since(td),
            bytes.len()
        );
        Ok(bytes)
    }
}
