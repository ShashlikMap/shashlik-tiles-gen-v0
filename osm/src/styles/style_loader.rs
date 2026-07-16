use crate::source::reqwest_source::ReqwestSource;
use crate::styles::Style;
use log::error;
use std::fs;
use thiserror::Error;

pub struct StyleLoader;

#[derive(Debug, Error)]
pub enum StylesFetchError {
    #[error("Internal")]
    Internal,
}

impl StyleLoader {
    pub fn load(local_file: bool) -> Vec<Style> {
        if local_file {
            return serde_json::from_slice(fs::read("styles_v0.json").as_ref().unwrap()).unwrap();
        }

        let styles = ReqwestSource::new().styles();
        if let Err(err) = styles.as_ref() {
            error!("Error loading styles: {:?}", err);
        }
        styles.unwrap_or_default()
    }
}
