use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TempCountries {
    #[serde(rename = "ref_country_codes")]
    pub ref_country_codes: Vec<RefCountryCode>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefCountryCode {
    pub country: String,
    pub alpha2: String,
    pub alpha3: String,
    pub numeric: i64,
    pub latitude: f64,
    pub longitude: f64,
}
