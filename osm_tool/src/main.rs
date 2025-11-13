pub mod extract;
pub mod filter;
pub mod proto;
pub mod reader;
pub mod tags;
mod config;
mod countries;
mod tile_processor;
mod shape_processor;
mod pbf_processor;
mod way_store;
mod polygon_store;

use clap::{Args, Parser, Subcommand};
use geo::{Coord, CoordNum, Rect};

use crate::config::ShashlikConfig;
use crate::pbf_processor::PbfProcessor;
use crate::shape_processor::ShapeProcessor;
use crate::tile_processor::TileProcessor;
use itertools::Itertools;
use osm::map::{get_world_boundary, HighwayKind};
use osm::routing::OsmRoadGraph;
use reader::OsmNode;
use rs_concaveman::location_trait::LocationTrait;
use std::time::Instant;
use std::{collections::HashMap, fs::File};

#[derive(Parser)]
#[command(about = "OSM data manipulation tool")]
struct OsmToolCommand {
    #[command(subcommand)]
    subcommand: OsmToolSubcommand,
}

// TODO How to get rid of this?
#[derive(Eq, PartialEq, Clone, Copy, Debug, Hash, Default)]
pub struct LocationTraitCoord<T: CoordNum = f64> {
    pub coord: Coord<T>,
}
impl LocationTrait for LocationTraitCoord {
    fn get_x(&self) -> f64 {
        self.coord.x
    }

    fn get_y(&self) -> f64 {
        self.coord.y
    }
}

#[derive(Args)]
struct ExtractArgs {
    /// Path to shashlik config json file.
    /// Example of json:
    /// { land_path: "/Users/kirill/Downloads/japan-latest.osm.pbf", areas: [Area { name: "Tokyo", enabled: true, path: "/Users/kirill/Downloads/japan-latest.osm.pbf", left: 138.647, top: 36.532, right: 140.933, bottom: 34.574 }, Area { name: "San Francisco", enabled: true, path: "/Users/kirill/Downloads/norcal-latest.osm.pbf", left: -122.5456, top: 37.8141, right: -121.7752, bottom: 37.2325 }, Area { name: "London", enabled: true, path: "/Users/kirill/Downloads/greater-london-latest.osm.pbf", left: -0.2705, top: 51.5775, right: 0.0858, bottom: 51.4232 }] }
    shashlik_config_path: String,
}

#[derive(Args)]
struct RoadGraphArgs {
    /// Path to OSM file
    osm_file_path: String,
    /// Road graph DB path for output
    graph_db_path: String,
}

#[derive(Subcommand)]
enum OsmToolSubcommand {
    #[command(about = "Extract OSM spacial/vector data")]
    Extract(ExtractArgs),
    #[command(about = "Compute road graph for routing")]
    RoadGraph(RoadGraphArgs),
}

const POLYGON_MERGE_ZOOM_LEVEL: u32 = 3;

fn main() {
    let cmd = OsmToolCommand::parse();

    match cmd.subcommand {
        OsmToolSubcommand::RoadGraph(args) => {
            let f = File::open(args.osm_file_path).expect("Could not open OSM file");
            let mut graph = OsmRoadGraph::default();
            let mut nodes: HashMap<i64, OsmNode> = HashMap::new();

            for blob in reader::OsmReader::new(f, get_world_boundary()) {
                print!("*");
                if let reader::OsmBlob::Data(data_blob) = blob.expect("Failed to read blob") {
                    let highway_filter = filter::TagFilter::new(
                        &data_blob.string_table,
                        &[("highway", None),],
                    );
                    let direction_filter = filter::TagFilter::new(
                        &data_blob.string_table,
                        &[("oneway", Some("yes")),],
                    );

                    nodes.extend(data_blob.nodes.iter().map(|node| (node.id, node.clone())));

                    if data_blob.ways.is_empty() && data_blob.relations.is_empty() {
                        continue;
                    }

                    for way in data_blob.ways {

                        if let Some((k, v)) = highway_filter.filter(&data_blob.string_table, &way.tags)
                        {
                            match k {
                                "highway" => {
                                    let hkind = match HighwayKind::from_descr(v) {
                                        Some(hk) => hk,
                                        None => continue
                                    };
                                    let one_direction = direction_filter.filter(&data_blob.string_table, &way.tags).is_some();
                                    let path = way.refs
                                        .into_iter()
                                        .filter_map(|nid| nodes.get(&nid))
                                        .tuple_windows::<(_, _)>();

                                    for (a, b) in path {
                                        if one_direction {
                                            graph.add_edge((a.id, a.coord), (b.id, b.coord), hkind);
                                        } else {
                                            graph.add_bi_edge((a.id, a.coord), (b.id, b.coord), hkind);
                                        }
                                    }
                                },
                                _ => {}
                            }
                        }
                    }
                }
            }

            graph.save(&args.graph_db_path).unwrap();
        }
        OsmToolSubcommand::Extract(args) => {
            let shashlik_config: ShashlikConfig =
                serde_json::from_reader(File::open(args.shashlik_config_path).unwrap()).expect("JSON was not well-formatted");
            println!("shashlik_config: {:?}", shashlik_config);

            let extract_ts = Instant::now();

            let mut tile_processor = TileProcessor::new();
            let shape_processor = ShapeProcessor {
                world_boundary: get_world_boundary()
            };

            for area in shashlik_config.areas {
                if !area.enabled {
                    println!("Area {} disabled", area.name);
                    continue;
                }
                let osm_file = File::open(area.path).expect("Could not open OSM file");
                println!("Extracting OSM data for {}", area.name);
                let boundary = Rect::new(
                    Coord {
                        x: area.left,
                        y: area.top,
                    },
                    Coord {
                        x: area.right,
                        y: area.bottom,
                    },
                );
                let mut pbf_processor = PbfProcessor::new();
                pbf_processor.process_pbf(boundary, osm_file, &mut tile_processor,
                                          shashlik_config.merge_polygons,
                                          shashlik_config.preserve_road_topology);
            }

            if shashlik_config.planet_data {
                shape_processor.extract_planet_data(&mut tile_processor)
            }

            tile_processor.save_to_disk();

            println!("Total extract time: {:?}", extract_ts.elapsed());
        }
    }
}




