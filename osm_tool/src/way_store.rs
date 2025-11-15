use geo::line_measures::LengthMeasurable;
use geo::{Coord, Euclidean, LineString, Simplify};
use itertools::Itertools;
use osm::map::LineKind::{Highway, Railway};
use osm::map::{HighwayKind, LineKind, MapGeomObject, MapGeomObjectKind, MapGeometry, RailwayKind, WayInfo, ZOOM_LEVELS};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::Sender;

#[derive(Clone)]
pub struct WayStoreItem {
    pub f_id: i64,
    pub l_id: i64,
    pub way_id: i64,
    pub line: LineString,
    pub info: WayInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CoordInt {
    pub x: i64,
    pub y: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PathNodeKey {
    pub id: i64,
    pub info: WayInfo,
}

#[derive(Debug, Clone)]
struct PathNode {
    pub f_id: i64,
    pub l_id: i64,
    pub data: (MapGeomObject, LineString),
}

pub struct WayStore {
    items: Vec<WayStoreItem>,
}

impl WayStore {
    pub fn new() -> Self {
        WayStore { items: vec![] }
    }
    pub fn add_item(&mut self, way_store_item: WayStoreItem) {
        self.items.push(way_store_item);
    }

    pub fn process_ways_async(
        &self,
        sender: Sender<(u32, MapGeomObject, MapGeometry)>,
        preserve_topology: bool,
    ) {
        let items = self.items.clone();
        std::thread::spawn(move || {
            Self::process_ways(sender, preserve_topology, items);
        });
    }

    fn process_ways(
        sender: Sender<(u32, MapGeomObject, MapGeometry)>,
        preserve_topology: bool,
        items: Vec<WayStoreItem>,
    ) {
        println!("Process ways");
        let merged_ways = Self::merge_ways(items, &[
            Highway { kind: HighwayKind::Footway }
        ]);

        if preserve_topology {
            Self::process_with_preserve_topology(sender, merged_ways);
        } else {
            Self::process_without_preserve_topology(sender, merged_ways);
        }
    }

    fn process_without_preserve_topology(
        sender: Sender<(u32, MapGeomObject, MapGeometry)>,
        data: Vec<(MapGeomObject, LineString)>,
    ) {
        for (map_geom_obj, line) in data {
            let mut temp_line = line;
            for zoom_level in 0..ZOOM_LEVELS {
                let included = match &map_geom_obj.kind {
                    MapGeomObjectKind::Way(info) => {
                        if zoom_level == 0 {
                            true
                        } else if zoom_level <= 1 {
                            info.line_kind
                                != (Highway {
                                    kind: HighwayKind::Footway,
                                })
                        } else if info.line_kind
                            == (Railway {
                                kind: RailwayKind::Rail,
                            })
                        {
                            zoom_level < 4
                        } else if zoom_level >= 13 {
                            false
                        } else {
                            let line_kind_layer = info.line_kind.get_layer();
                            zoom_level <= 4 && line_kind_layer >= 12
                                || zoom_level <= 5 && line_kind_layer >= 13
                                || zoom_level <= 6 && line_kind_layer >= 14
                                || zoom_level <= 8 && line_kind_layer >= 15
                                || line_kind_layer >= 16
                        }
                    }
                    _ => false,
                };
                if included {
                    let zlf = zoom_level as f64;
                    let line = if temp_line.0.len() > 2 {
                        temp_line.simplify(0.000008 * zlf * zlf)
                    } else {
                        temp_line.clone()
                    };

                    temp_line = line.clone();
                    sender
                        .send((zoom_level, map_geom_obj.clone(), MapGeometry::Line(line)))
                        .unwrap();
                } else {
                    break;
                }
            }
        }
    }

    fn process_with_preserve_topology(
        sender: Sender<(u32, MapGeomObject, MapGeometry)>,
        data: Vec<(MapGeomObject, LineString)>,
    ) {
        let mut seen = FxHashSet::default();

        // TODO Need to rewrite to support only preserve_topology case here
        let preserve_topology = true;

        // TODO It should be possible to combine both maps here but something goes wrong with algo
        // Note: for some reason FxHashMap takes much more time here
        let mut start_end_map: HashMap<CoordInt, i32> = HashMap::new();
        let mut nodes_counter: HashMap<CoordInt, i32> = HashMap::new();
        if preserve_topology {
            data.iter().for_each(|(_, line)| {
                *start_end_map
                    .entry(Self::create_coord_id(line.0.first().unwrap()))
                    .or_insert(0) += 1;
                *start_end_map
                    .entry(Self::create_coord_id(line.0.last().unwrap()))
                    .or_insert(0) += 1;
                line.coords().for_each(|coord| {
                    *nodes_counter
                        .entry(Self::create_coord_id(coord))
                        .or_insert(0) += 1;
                });
            });
        }

        for zoom_level in 0..ZOOM_LEVELS {
            let filtered = data
                .iter()
                .filter_map(|(map_geom_obj, line)| {
                    if zoom_level == 0 {
                        Some((map_geom_obj.clone(), line))
                    } else {
                        if preserve_topology && seen.contains(&map_geom_obj.id) {
                            return None;
                        }
                        let included = match &map_geom_obj.kind {
                            MapGeomObjectKind::Way(info) => {
                                if zoom_level <= 1 {
                                    info.line_kind
                                        != (Highway {
                                            kind: HighwayKind::Footway,
                                        })
                                } else if info.line_kind
                                    == (Railway {
                                        kind: RailwayKind::Rail,
                                    })
                                {
                                    zoom_level < 4
                                } else if zoom_level >= 13 {
                                    false
                                } else {
                                    let line_kind_layer = info.line_kind.get_layer();
                                    zoom_level <= 4 && line_kind_layer >= 12
                                        || zoom_level <= 5 && line_kind_layer >= 13
                                        || zoom_level <= 6 && line_kind_layer >= 14
                                        || zoom_level <= 8 && line_kind_layer >= 15
                                        || line_kind_layer >= 16
                                }
                            }
                            _ => false,
                        };

                        if included {
                            Some((map_geom_obj.clone(), line))
                        } else {
                            if preserve_topology {
                                seen.insert(map_geom_obj.id);

                                *start_end_map
                                    .entry(Self::create_coord_id(line.0.first().unwrap()))
                                    .or_insert(1) -= 1;
                                *start_end_map
                                    .entry(Self::create_coord_id(line.0.last().unwrap()))
                                    .or_insert(1) -= 1;

                                line.coords().for_each(|coord| {
                                    *nodes_counter
                                        .entry(Self::create_coord_id(coord))
                                        .or_insert(1) -= 1;
                                });
                            }
                            None
                        }
                    }
                })
                .collect_vec();

            let mut way_nodes_for_level = 0;
            let zlf = zoom_level as f64;

            for (map_geom_obj, line) in filtered {
                if zoom_level == 0 || !preserve_topology {
                    let line = if line.0.len() > 2 {
                        line.simplify(0.000008 * zlf * zlf)
                    } else {
                        line.clone()
                    };
                    way_nodes_for_level += line.0.len() as u32;

                    let geom = MapGeometry::Line(line);

                    sender
                        .send((zoom_level, map_geom_obj.clone(), geom))
                        .unwrap();
                } else {
                    let line_endings_connected = *nodes_counter
                        .entry(Self::create_coord_id(line.0.first().unwrap()))
                        .or_insert(0)
                        > 1
                        || *nodes_counter
                            .entry(Self::create_coord_id(line.0.last().unwrap()))
                            .or_insert(0)
                            > 1;

                    let mut temp = Vec::new();
                    let mut prev_index = 0;
                    let mut intersections = 0;
                    let line_length = line.length(&Euclidean);
                    line.coords().enumerate().for_each(|(index, coord)| {
                        temp.push(*coord);
                        if *start_end_map
                            .entry(Self::create_coord_id(coord))
                            .or_insert(0)
                            > 0
                            && temp.len() >= 2
                        {
                            if index == line.0.len() - 1
                                && prev_index == 0
                                && intersections == 0
                                && !line_endings_connected
                                && line_length <= 0.0025
                            {
                                // we drop here very short lines without any other connections.
                                // these are "leftovers" after filtering and create only visual noise.
                            } else {
                                prev_index = index;
                                let line = LineString(temp.clone());
                                let line = if line.0.len() > 2 {
                                    line.simplify(0.000008 * zlf * zlf)
                                } else {
                                    line.clone()
                                };

                                intersections += 1;
                                way_nodes_for_level += line.0.len() as u32;

                                let geom = MapGeometry::Line(line);
                                sender
                                    .send((zoom_level, map_geom_obj.clone(), geom))
                                    .unwrap();

                                temp.clear();
                                temp.push(*coord);
                            }
                        }
                    });
                }
            }
            println!(
                "way_nodes = {} for zoom {}",
                way_nodes_for_level, zoom_level
            );
        }
    }

    fn merge_ways(items: Vec<WayStoreItem>, exclude: &[LineKind]) -> Vec<(MapGeomObject, LineString)> {
        let excluded_set: HashSet<LineKind> = exclude.iter().cloned().collect();
        let mut excluded_vec = Vec::new();
        let mut merged_map: FxHashMap<PathNodeKey, PathNode> = FxHashMap::default();
        for item in items {
            let mut path = item.line;
            let map_geom_obj_kind = MapGeomObjectKind::Way(item.info.clone());

            let map_geom_obj = MapGeomObject {
                id: item.way_id,
                kind: map_geom_obj_kind,
            };

            if excluded_set.contains(&item.info.line_kind) {
                excluded_vec.push((map_geom_obj, path));
                continue;
            }

            let mut key1 = PathNodeKey {
                id: item.f_id,
                info: item.info.clone(),
            };
            let mut key2 = PathNodeKey {
                id: item.l_id,
                info: item.info.clone(),
            };

            loop {
                if let Some(node) = merged_map.remove(&key1) {
                    merged_map.remove(&PathNodeKey {
                        id: if node.f_id == key1.id {
                            node.l_id
                        } else {
                            node.f_id
                        },
                        info: item.info.clone(),
                    });
                    key1.id = if node.f_id == key1.id {
                        node.l_id
                    } else {
                        node.f_id
                    };
                    let mut new_vec = node.data.1.0.iter().rev().copied().collect_vec();
                    new_vec.extend(path);
                    path = LineString(new_vec);
                } else if let Some(node) = merged_map.remove(&key2) {
                    merged_map.remove(&PathNodeKey {
                        id: if node.f_id == key2.id {
                            node.l_id
                        } else {
                            node.f_id
                        },
                        info: item.info.clone(),
                    });
                    key2.id = if node.f_id == key2.id {
                        node.l_id
                    } else {
                        node.f_id
                    };
                    let mut new_vec = path.0;
                    new_vec.extend(node.data.1);
                    path = LineString(new_vec);
                } else {
                    break;
                }
            }

            // TODO After line merging the road name might be incorrect. Need to figure out how to tackle it.
            merged_map.insert(
                key2.clone(),
                PathNode {
                    f_id: key2.id,
                    l_id: key1.id,
                    data: (
                        map_geom_obj.clone(),
                        LineString(path.0.iter().rev().copied().collect_vec()),
                    ),
                },
            );
            merged_map.insert(
                key1.clone(),
                PathNode {
                    f_id: key1.id,
                    l_id: key2.id,
                    data: (map_geom_obj, path),
                },
            );
        }

        excluded_vec.into_iter().chain(merged_map.into_values()
            .map(|node| node.data)
            .unique_by(|data| data.0.id).into_iter())
            .collect_vec()
    }

    fn create_coord_id(coord: &Coord) -> CoordInt {
        CoordInt {
            x: (coord.x * 1000000000000.0) as i64,
            y: (coord.y * 1000000000000.0) as i64,
        }
    }
}
