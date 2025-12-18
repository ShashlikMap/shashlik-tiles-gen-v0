use crate::filter::TagFilter;
use crate::polygon_store::PolygonStore;
use crate::reader::OsmBlobData;
use crate::tile_processor::TileProcessor;
use crate::way_store::{WayStore, WayStoreItem};
use crate::{reader, POLYGON_MERGE_ZOOM_LEVEL};
use geo::{Contains, Coord, HasDimensions, LineString, Polygon, Rect};
use itertools::Itertools;
use osm::map::LineKind::Railway;
use osm::map::NatureKind::Forest;
use osm::map::{
    HighwayKind, LayerKind, LineKind, MapGeomObject, MapGeomObjectKind, MapGeometry,
    RailwayKind, WayInfo,
};
use rustc_hash::FxHashMap;
use std::fs::File;
use std::sync::mpsc::{channel, Sender};
use std::sync::{mpsc, Arc};
use std::time::Instant;

pub struct PbfProcessor {
    way_store: WayStore,
    polygon_store: PolygonStore,
}

impl PbfProcessor {
    pub fn new() -> PbfProcessor {
        PbfProcessor {
            way_store: WayStore::new(),
            polygon_store: PolygonStore::new(),
        }
    }

    const POI_TAG: &'static [(&'static str, Option<&'static str>)] = &[
        ("highway", Some("traffic_signals")),
        ("amenity", Some("toilets")),
        ("amenity", Some("parking")),
        ("railway", Some("station")),
    ];
    const RELATION_TAG: &'static [(&'static str, Option<&'static str>)] = &[
        ("water", None),
        ("natural", Some("wood")),
        ("natural", Some("water")),
        ("natural", Some("bay")),
        ("landuse", Some("grass")),
        ("landuse", Some("forest")),
    ];

    const WAYS_TAG: &'static [(&'static str, Option<&'static str>)] = &[
        ("railway", Some("rail")),
        ("highway", Some("motorway")),
        ("highway", Some("trunk")),
        ("highway", Some("primary")),
        ("highway", Some("secondary")),
        ("highway", Some("tertiary")),
        ("highway", Some("unclassified")),
        ("highway", Some("residential")),
        ("highway", Some("motorway_link")),
        ("highway", Some("trunk_link")),
        ("highway", Some("primary_link")),
        ("highway", Some("secondary_link")),
        ("highway", Some("tertiary_link")),
        ("highway", Some("service")),
        ("highway", Some("footway")),
        ("water", None),
        ("leisure", Some("park")),
        ("natural", Some("wood")),
        ("natural", Some("water")),
        ("landuse", Some("forest")),
        ("landuse", Some("grass")),
        ("building", Some("yes")),
        ("building", Some("commercial")),
        ("building", Some("industrial")),
    ];

    pub fn process_pbf(
        &mut self,
        boundary: Rect,
        osm_file: File,
        tile_processor: &mut TileProcessor,
        merge_polygons: bool,
        preserve_roads_topology: bool,
    ) {
        let t_start = Instant::now();
        let mut blob_index = 0;
        let mut reader = reader::OsmReader::new(osm_file, boundary);
        let mut nodes: FxHashMap<i64, Coord> = FxHashMap::default();
        let mut ways: FxHashMap<i64, Vec<i64>> = FxHashMap::default();

        // TODO how to keep it inside Reader?
        let used_ways_ids = reader.extract_ways_id_from_relations(Self::RELATION_TAG);
        let (node_blobs, way_blobs, rels_blobs) = reader.data();
        for data_blob in node_blobs {
            blob_index += 1;
            print!("\rProcessing blob: {}", blob_index);
            Self::read_nodes(tile_processor, &data_blob, &mut nodes);
        }

        let tp = threadpool::ThreadPool::new(6);
        let nodes = Arc::new(nodes);

        let (tx, rx) = mpsc::channel();
        for data_blob in way_blobs {
            blob_index += 1;
            print!("\rProcessing blob: {}", blob_index);
            for way in &data_blob.ways {
                if used_ways_ids.contains(&way.id) {
                    ways.insert(way.id, way.refs.clone());
                }
            }
            let nodes = Arc::clone(&nodes);
            let tx = tx.clone();
            tp.execute(move || {
                Self::read_ways(tx, data_blob, &nodes);
            });
        }
        drop(tx);
        for (way_store_item, tile_item) in rx {
            if let Some(way_store_item) = way_store_item {
                self.way_store.add_item(way_store_item);
            } else if let Some(tile_item) = tile_item {
                self.handle_tile_item(tile_item, tile_processor);
            }
        }

        let (tx, rx) = mpsc::channel();
        let ways = Arc::new(ways);
        for data_blob in rels_blobs {
            blob_index += 1;
            print!("\rProcessing blob: {}", blob_index);
            let nodes = Arc::clone(&nodes);
            let ways = Arc::clone(&ways);
            let tx = tx.clone();
            tp.execute(move || {
                Self::read_relations(tx, &ways, data_blob, &nodes);
            });
        }
        drop(tx);
        for tile_item in rx {
            self.handle_tile_item(tile_item, tile_processor);
        }

        print!("\rBlobs processed: {:?}\n", t_start.elapsed());

        self.process_ways_and_forest(tile_processor, merge_polygons, preserve_roads_topology);
    }

    fn process_ways_and_forest(
        &mut self,
        tile_processor: &mut TileProcessor,
        merge_polygons: bool,
        preserve_roads_topology: bool,
    ) {
        let process_start_t = Instant::now();
        let (tx, rx) = channel::<(u32, MapGeomObject, MapGeometry)>();
        self.polygon_store.process_forests_async(
            tx.clone(),
            merge_polygons,
            POLYGON_MERGE_ZOOM_LEVEL,
        );
        self.way_store
            .process_ways_async(tx, preserve_roads_topology);
        for tile_data in rx {
            let (zoom, geom_obj, geom) = tile_data;
            tile_processor
                .tile_writer
                .add_to_tiles(zoom, geom_obj, geom, true);
        }

        println!("Process finished: {:?}", process_start_t.elapsed());
    }

    fn handle_tile_item(
        &mut self,
        tile_item: (MapGeomObject, MapGeometry),
        tile_processor: &mut TileProcessor,
    ) {
        let (map_geom_obj, geom_obj) = tile_item;
        if map_geom_obj.kind == MapGeomObjectKind::Nature(Forest) {
            match &geom_obj {
                MapGeometry::Poly(ref poly) => {
                    self.polygon_store
                        .add_polygon(Polygon::new(poly.exterior().clone(), vec![]));
                }
                _ => {}
            }
        }
        tile_processor.add_to_tiles(map_geom_obj, geom_obj);
    }

    fn read_relations(
        sender: Sender<(MapGeomObject, MapGeometry)>,
        ways: &Arc<FxHashMap<i64, Vec<i64>>>,
        data_blob: OsmBlobData,
        nodes: &Arc<FxHashMap<i64, Coord>>,
    ) {
        let tag_filter = TagFilter::new(&data_blob.string_table, Self::RELATION_TAG);
        for relation in &data_blob.relations {
            if let Some((k, v)) = tag_filter.filter(&data_blob.string_table, &relation.tags) {
                let mut all_outer_ways: Vec<Vec<i64>> = Vec::new();
                let mut all_inner_ways: Vec<Vec<i64>> = Vec::new();

                for way_tuple in relation.ways.iter() {
                    let (id, role) = way_tuple;
                    let role_value = data_blob.string_table[role.clone() as usize].as_str();
                    if let Some(refs) = ways.get(&id) {
                        if !refs.is_empty() {
                            if role_value == "inner" {
                                all_inner_ways.push(refs.clone());
                            } else if role_value == "outer" {
                                all_outer_ways.push(refs.clone());
                            }
                        }
                    }
                }

                if all_outer_ways.is_empty() {
                    continue;
                }

                let mut hm = FxHashMap::default();
                for mut way in all_outer_ways {
                    let mut key1 = way.first().cloned().unwrap();
                    let mut key2 = way.last().cloned().unwrap();
                    while hm.contains_key(&key1) || hm.contains_key(&key2) {
                        if let Some(w) = hm.remove(&key1) {
                            let mut nw = Vec::new();
                            nw.extend(w);
                            nw.extend(way);
                            way = nw;
                            key1 = *way.first().unwrap();
                            key2 = *way.last().unwrap();
                        } else if let Some(w) = hm.remove(&key2) {
                            let mut nw = Vec::new();
                            nw.extend(w);
                            nw.extend(way.into_iter().rev());
                            way = nw;
                            key1 = *way.first().unwrap();
                            key2 = *way.last().unwrap();
                        }
                    }

                    hm.insert(key2, way);
                }
                let mut polygons: Vec<Polygon> = hm
                    .values()
                    .map(|way| {
                        let coords = way
                            .iter()
                            .map(|id| nodes.get(id).unwrap().clone())
                            .collect_vec();

                        Polygon::new(LineString(coords), Vec::new()).into()
                    })
                    .collect_vec();

                for line in all_inner_ways {
                    if line.len() > 0 {
                        if let Some(fc) = nodes.get(line.first().unwrap()) {
                            for poly in &mut polygons {
                                if poly.contains(fc) {
                                    let coords = line
                                        .iter()
                                        .map(|id| nodes.get(id).unwrap().clone())
                                        .collect_vec();
                                    let ls = LineString(coords);
                                    poly.interiors_push(ls);
                                    break;
                                }
                            }
                        }
                    }
                }

                for polygon in polygons {
                    let map_geom_obj = MapGeomObject {
                        id: relation.id,
                        kind: MapGeomObjectKind::from_tag(k, v, None, None, None, false),
                    };
                    sender
                        .send((map_geom_obj, MapGeometry::Poly(polygon)))
                        .unwrap();
                }
            }
        }
    }
    
    fn read_ways(
        sender: Sender<(Option<WayStoreItem>, Option<(MapGeomObject, MapGeometry)>)>,
        data_blob: OsmBlobData,
        nodes: &Arc<FxHashMap<i64, Coord>>,
    ) {
        let tag_filter = TagFilter::new(&data_blob.string_table, Self::WAYS_TAG);
        let road_tag_filter = TagFilter::new(
            &data_blob.string_table,
            &[("layer", None), ("tunnel", Some("yes")), ("bridge", None), ("name:en", None), ("name", None)],
        );
        let building_tag_filter = TagFilter::new(
            &data_blob.string_table,
            &[("building:levels", None)],
        );

        for way in &data_blob.ways {
            if let Some((k, v)) = tag_filter.filter(&data_blob.string_table, &way.tags) {
                match k {
                    "railway" | "highway" => {
                        let path_data = way.as_line(&nodes);

                        let path = path_data.0;

                        if path.is_empty() {
                            continue;
                        }

                        let mut layer = 0;
                        let mut layer_kind = LayerKind::None;
                        let mut road_name_en:Option<String> = None;
                        let mut road_name:Option<String> = None; 

                        for (k, v) in
                            road_tag_filter.filter_all(&data_blob.string_table, &way.tags)
                        {
                            match k {
                                "layer" => {
                                    layer = v.parse::<i32>().unwrap_or(0);
                                }
                                "tunnel" => {
                                    layer_kind = LayerKind::Tunnel;
                                }
                                "bridge" => {
                                    layer_kind = LayerKind::Bridge;
                                }
                                "name:en" => {
                                    road_name_en = v.parse::<String>().ok();
                                }
                                "name" => {
                                    road_name = v.parse::<String>().ok();
                                }
                                _ => {}
                            }
                        }

                        // ignore layer if there is no bridge/tunnel
                        // based on osm wiki it's invalid
                        if layer_kind == LayerKind::None {
                            layer = 0;
                        }

                        // TODO Potentially, the opposite situation is similar when there is only tag without layer value
                        // Need to investigate
                        // if layer == 0 {
                        //     layer_kind = LayerKind::None
                        // }

                        let line_kind = if v == "rail" {
                            Railway {
                                kind: RailwayKind::Rail,
                            }
                        } else {
                            LineKind::Highway {
                                kind: HighwayKind::from_descr(v).unwrap(),
                            }
                        };

                        let way_info = WayInfo {
                            line_kind,
                            layer,
                            layer_kind,
                            name_en: road_name_en.or(road_name),
                        };

                        sender
                            .send((
                                Some(WayStoreItem {
                                    f_id: path_data.1,
                                    l_id: path_data.2,
                                    way_id: way.id,
                                    line: path,
                                    info: way_info,
                                }),
                                None,
                            ))
                            .unwrap();
                    }
                    _ => {
                        let polygon = way.as_polygon(&nodes);

                        if polygon.is_empty() {
                            continue;
                        }

                        let levels = if k == "building" {
                            let mut levels = 0;
                            for (k, v) in
                                building_tag_filter.filter_all(&data_blob.string_table, &way.tags)
                            {
                                match k {
                                    "building:levels" => {
                                        levels = v.parse::<u16>().unwrap_or(0);
                                    }
                                    _ => {}
                                }
                            }
                            Some(levels)
                        } else {
                            None
                        };

                        let map_geom_obj = MapGeomObject {
                            id: way.id,
                            kind: MapGeomObjectKind::from_tag(k, v, None, None, levels, false),
                        };

                        sender
                            .send((None, Some((map_geom_obj, MapGeometry::Poly(polygon)))))
                            .unwrap();
                    }
                }
            }
        }
    }

    fn read_nodes(
        tile_processor: &mut TileProcessor,
        data_blob: &OsmBlobData,
        nodes: &mut FxHashMap<i64, Coord>,
    ) {
        let tag_filter = TagFilter::new(&data_blob.string_table, Self::POI_TAG);
        let name_en_tag_filter = TagFilter::new(
            &data_blob.string_table, &[("name:en", None), ("name", None)],
        );
        let train_tag_filter = TagFilter::new(
            &data_blob.string_table, &[("train", Some("yes"))],
        );
        for node in &data_blob.nodes {
            nodes.insert(node.id, node.coord);

            if let Some((k, v)) = tag_filter.filter(&data_blob.string_table, &node.tags) {
                let mut name_en: Option<String> = None;
                let mut name: Option<String> = None;
                for (k, v) in
                    name_en_tag_filter.filter_all(&data_blob.string_table, &node.tags)
                {
                    match k {
                        "name:en" => {
                            name_en = v.parse::<String>().ok();
                        }
                        "name" => {
                            name = v.parse::<String>().ok();
                        }
                        _ => {}
                    }
                }

                let is_train = train_tag_filter.filter(&data_blob.string_table, &node.tags).is_some();
                let map_geom_obj = MapGeomObject {
                    id: node.id,
                    kind: MapGeomObjectKind::from_tag(k, v, None, name_en.or(name), None, is_train),
                };

                tile_processor.add_to_tiles(map_geom_obj, MapGeometry::Coord(node.coord));
            }
        }
    }
}
