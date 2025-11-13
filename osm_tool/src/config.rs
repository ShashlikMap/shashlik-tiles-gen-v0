use serde::Deserialize;
use serde_derive::Serialize;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShashlikConfig {
    #[serde(rename = "land_path")]
    pub land_path: String,
    #[serde(rename = "planet_data")]
    pub planet_data: bool,
    #[serde(rename = "merge_polygons")]
    pub merge_polygons: bool,
    #[serde(rename = "preserve_road_topology")]
    pub preserve_road_topology: bool,
    pub areas: Vec<Area>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Area {
    pub name: String,
    pub enabled: bool,
    pub path: String,
    pub left: f64,
    pub top: f64,
    pub right: f64,
    pub bottom: f64,
}
