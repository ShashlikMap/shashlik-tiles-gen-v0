use crate::source::reqwest_source::ReqwestSource;
use crate::styles::Style;
use log::error;
use thiserror::Error;

pub struct StyleLoader;

#[derive(Debug, Error)]
pub enum StylesFetchError {
    #[error("Internal")]
    Internal,
}

impl StyleLoader {
    pub fn load() -> Vec<Style> {
        let styles = ReqwestSource::new().styles();
        if let Err(err) = styles.as_ref() {
            error!("Error loading styles: {:?}", err);
        }
        styles.unwrap_or_default()
    }
}
