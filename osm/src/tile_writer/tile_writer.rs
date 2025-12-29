use crate::map::{
    MapGeomObject, MapGeometry, MapGeometryCollection, DBS_FOLDER,
};
use crate::tile_writer::sutherland_hodgman::sutherland_hodgman_clip;
use crate::tiles::{
    calc_tile_ranges, create_tiles_db_connection, TileKey, TileRanges, TILES_COUNT,
};
use flate2::write::GzEncoder;
use flate2::Compression;
use geo::line_intersection::line_intersection;
use geo::{
    coord, BoundingRect, Contains, Coord, Intersects, Line, LineIntersection, LineString,
    MapCoords, MapCoordsInPlace, MultiLineString, Polygon, Rect,
};
use googleprojection::Mercator;
use itertools::Itertools;
use rusqlite::{Connection, Transaction};
use rustc_hash::{FxHashMap, FxHashSet};
use std::io::Write;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::{fs, io};
use threadpool::ThreadPool;

pub struct TileWriter {
    thread_pool: ThreadPool,
    sender: Option<Sender<(TileKey, MapGeomObject, MapGeometry)>>,
    receiver: Receiver<(TileKey, MapGeomObject, MapGeometry)>,
    tile_db_map: FxHashMap<TileKey, MapGeometryCollection>,
    tile_keys_cache: Arc<FxHashSet<TileKey>>,
}

impl Default for TileWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl TileWriter {
    const MIN_ZOOM_FOR_PLANET_TILES: u32 = 10;
    pub fn new() -> Self {
        let (tx, rx) = channel::<(TileKey, MapGeomObject, MapGeometry)>();
        TileWriter {
            thread_pool: ThreadPool::new(3),
            sender: Some(tx),
            receiver: rx,
            tile_db_map: FxHashMap::default(),
            tile_keys_cache: Arc::new(FxHashSet::default()),
        }
    }

    pub fn add_to_tiles(
        &mut self,
        zoom_level: u32,
        map_geom_object: MapGeomObject,
        map_geometry: MapGeometry,
        can_create_new_tiles: bool,
    ) {
        // if !can_create_new_tiles && zoom_level >= Self::MIN_ZOOM_FOR_GEOM_INDEX_DB {
        //     let key = MapIndexKey::new(zoom_level);
        //     let geom_index = self.geom_db_map.entry(key).or_default();
        //     geom_index.db_insert(map_geom_object.clone(), map_geometry.clone());
        // }

        if !can_create_new_tiles && self.tile_keys_cache.is_empty() {
            let mut temp_map = FxHashSet::default();
            temp_map.extend(self.tile_db_map.keys().copied());
            self.tile_keys_cache = Arc::new(temp_map);
        }

        let sender = self.sender.clone().unwrap();
        let tile_keys_cache = Arc::clone(&self.tile_keys_cache);

        self.thread_pool.execute(move || {
            let geom_rect = &map_geometry.bounding_rect().unwrap();
            let ranges = calc_tile_ranges(TILES_COUNT, zoom_level as i32, geom_rect);
            Self::fill_map(
                &tile_keys_cache,
                sender,
                zoom_level as i32,
                &map_geom_object,
                map_geometry,
                geom_rect,
                ranges,
                can_create_new_tiles,
            );
        });
    }

    fn fill_map(
        keys_cache: &Arc<FxHashSet<TileKey>>,
        sender: Sender<(TileKey, MapGeomObject, MapGeometry)>,
        zoom_level: i32,
        map_geom_object: &MapGeomObject,
        map_geometry: MapGeometry,
        geom_rect: &Rect,
        tile_ranges: TileRanges,
        force: bool,
    ) {
        for i in tile_ranges.min_x..tile_ranges.max_x + 1 {
            for j in tile_ranges.min_y..tile_ranges.max_y + 1 {
                let key = TileKey::new(i as i32, j as i32, zoom_level);
                if force
                    || zoom_level >= Self::MIN_ZOOM_FOR_PLANET_TILES as i32
                    || keys_cache.contains(&key)
                {
                    // increase a bit bbox for tile to reduce borders artefacts.
                    // it's a good tradeoff between amount artefacts and over tesselation/drawing
                    // note: caching this calculation isn't helpful

                    let tile_rect = key.calc_tile_boundary(1.01);

                    for item in Self::intersection(&map_geometry, &tile_rect, geom_rect) {
                        sender.send((key, map_geom_object.clone(), item)).unwrap();
                    }
                }
            }
        }
    }

    fn intersection(
        map_geometry: &MapGeometry,
        tile_rect: &Rect,
        geom_rect: &Rect,
    ) -> Vec<MapGeometry> {
        // quickly check if geometry is fully inside the tile
        if tile_rect.contains(geom_rect) {
            return vec![map_geometry.clone()];
        }
        // quickly check if geometry is fully outside the tile
        if !geom_rect.intersects(tile_rect) {
            return vec![];
        }
        match map_geometry {
            MapGeometry::Line(line) => {
                let mut multi_line = MultiLineString(Vec::new());
                let mut new_line = Vec::new();
                let coords = line.coords().collect_vec();
                for (index, &coord) in coords.iter().enumerate() {
                    if index > 0 {
                        let line = Line::new(*coords[index - 1], *coord);
                        if !tile_rect.contains(&line.start)
                            && !tile_rect.contains(&line.end)
                            && Self::rect_intersection(tile_rect, &line).is_none()
                        {
                            if new_line.len() > 1 {
                                multi_line.0.push(LineString(new_line));
                            }
                            new_line = Vec::new();
                        }
                    }
                    new_line.push(*coord)
                }
                if new_line.len() > 1 {
                    multi_line.0.push(LineString(new_line));
                }
                multi_line.0.into_iter().map(MapGeometry::Line).collect()
            }
            MapGeometry::Poly(poly) => {
                let exterior = poly.exterior();

                if let Some(intersected) = sutherland_hodgman_clip(exterior, tile_rect) {
                    let intersected_inters = poly
                        .interiors()
                        .iter()
                        .flat_map(|interior| sutherland_hodgman_clip(interior, tile_rect))
                        .collect_vec();
                    vec![MapGeometry::Poly(Polygon::new(
                        intersected,
                        intersected_inters,
                    ))]
                } else {
                    vec![]
                }

                // TODO make boolean intersection available with config a bit later
                // let intersected = poly.clone().intersection(&rect.to_polygon());
                // intersected.0.iter().map(|item| MapGeometry::Poly(item.clone())).collect()
            }
            MapGeometry::Coord(_) => vec![map_geometry.clone()],
        }
    }

    fn rect_intersection(rect: &Rect, line: &Line) -> Option<Coord> {
        for rect_line in rect.to_lines() {
            if let Some(intersection) = line_intersection(rect_line, *line) {
                match intersection {
                    LineIntersection::SinglePoint { intersection, .. } => {
                        return Some(intersection);
                    }
                    LineIntersection::Collinear { .. } => {}
                }
            }
        }
        None
    }

    pub fn flush_to_collections(&mut self, recreate_channel: bool) {
        self.sender = None;
        for data in &self.receiver {
            let collection = self.tile_db_map.entry(data.0).or_default();
            collection.0.push((data.1, data.2));
        }
        self.thread_pool.join();

        self.thread_pool = ThreadPool::new(2);
        if recreate_channel {
            let (tx, rx) = channel::<(TileKey, MapGeomObject, MapGeometry)>();
            self.sender = Some(tx);
            self.receiver = rx;
        }
    }

    pub fn save_to_file(&mut self) {
        println!("Saving all DBs");
        match fs::remove_dir_all(DBS_FOLDER) {
            Ok(_) => {}
            Err(_) => {
                println!("Failed to remove DBs");
            }
        }
        fs::create_dir_all(DBS_FOLDER).expect("Could not create dir dbs");

        self.flush_to_collections(false);
        let tile_db_map_len = self.tile_db_map.len();
        println!("tile_db_map len = {:?}", tile_db_map_len);

        let mut conn = Self::create_internal_tiles_db_connection();
        let tx = conn.transaction().unwrap();

        Self::perform_queries(&tx, &mut self.tile_db_map);

        tx.commit().unwrap();
    }

    fn perform_queries(
        tx: &Transaction,
        tile_db_map: &mut FxHashMap<TileKey, MapGeometryCollection>,
    ) {
        let mut stmt = tx
            .prepare("INSERT INTO tiles (x, y, z, data) VALUES (?1, ?2, ?3, ?4)")
            .unwrap();

        let len = tile_db_map.len();
        print!("Compressing: 0%");
        tile_db_map
            .iter_mut()
            .enumerate()
            .for_each(|(index, (key, data))| {
                data.0.sort_by(|(a, _), (b, _)| a.cmp(b));

                let tile_rect = key.calc_tile_boundary(1.0);
                let tile_rect_origin = Self::lat_lon_to_world(&tile_rect.min());
                data.0
                    .iter_mut()
                    .for_each(|(_, geometry)| Self::convert_coords(geometry, tile_rect_origin));

                let data = MapGeometryCollection::<f32>(
                    data.0
                        .iter()
                        .map(|(obj, geometry)| (obj.clone(), Self::convert_data(geometry)))
                        .collect(),
                );

                let serialized = bincode::serialize(&data).unwrap();
                let mut encoder = GzEncoder::new(Vec::new(), Compression::new(1));
                encoder.write_all(&serialized).unwrap();
                let compressed_data = encoder.finish().unwrap();
                stmt.execute((key.tile_x, key.tile_y, key.zoom_level, compressed_data))
                    .unwrap();

                let percent = ((index as f32 / len as f32) * 100.0).round() as i32;
                print!("\rCompressing: {}%", percent);
                io::stdout().flush().unwrap();
            });
    }

    fn convert_coords(geometry: &mut MapGeometry, tile_rect_origin: geo::Coord) {
        match geometry {
            MapGeometry::Line(line) => line.coords_mut().for_each(|coord| {
                *coord = Self::lat_lon_to_world(&coord) - tile_rect_origin;
            }),
            MapGeometry::Poly(poly) => {
                poly.map_coords_in_place(|coord| Self::lat_lon_to_world(&coord) - tile_rect_origin)
            }
            MapGeometry::Coord(coord) => *coord = Self::lat_lon_to_world(&coord) - tile_rect_origin,
        }
    }

    fn convert_data(geometry: &MapGeometry) -> MapGeometry<f32> {
        match geometry {
            MapGeometry::Line(line) => MapGeometry::Line(line.map_coords(|coord| {
                coord! {x: coord.x as f32, y: coord.y as f32}
            })),
            MapGeometry::Poly(poly) => MapGeometry::Poly(poly.map_coords(|coord| {
                coord! {x: coord.x as f32, y: coord.y as f32}
            })),
            MapGeometry::Coord(coord) => {
                MapGeometry::Coord(coord! {x: coord.x as f32, y: coord.y as f32})
            }
        }
    }

    fn lat_lon_to_world(lat_lon: &Coord<f64>) -> Coord<f64> {
        let lat_lon: (f64, f64) = (*lat_lon).into();
        Mercator::with_size(1)
            .from_ll_to_subpixel(&lat_lon, 22)
            .unwrap()
            .into()
    }

    fn create_internal_tiles_db_connection() -> Connection {
        let conn = create_tiles_db_connection();

        conn.execute("PRAGMA synchronous = OFF;", ()).unwrap();

        conn.execute("PRAGMA page_size = 65536;", ()).unwrap();

        conn.pragma_update(None, "journal_mode", "off").unwrap();

        conn.execute("VACUUM;", ()).unwrap();

        conn.execute("DROP TABLE IF EXISTS tiles;", ()).unwrap();

        conn.execute(
            "CREATE TABLE tiles (
                     x  INTEGER NOT NULL,
                     y  INTEGER NOT NULL,
                     z  INTEGER NOT NULL,
                     data  BLOB
                   )",
            (),
        )
        .unwrap();

        conn.execute("CREATE UNIQUE INDEX tiles_index ON tiles(x, y, z);", ())
            .unwrap();

        conn
    }
}
