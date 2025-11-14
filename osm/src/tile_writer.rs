use crate::map::{MapGeomObject, MapGeometry, MapGeometryCollection, DBS_FOLDER};
use crate::tiles::{
    calc_tile_ranges, create_tiles_db_connection, TileKey, TileRanges, TILES_COUNT,
};
use geo::{BoundingRect, Rect};
use rusqlite::{Connection, Transaction};
use rustc_hash::{FxHashMap, FxHashSet};
use std::{fs, io};
use std::io::Write;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use flate2::Compression;
use flate2::write::GzEncoder;
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
            let ranges = calc_tile_ranges(
                TILES_COUNT,
                zoom_level as i32,
                geom_rect,
            );
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
                if force || zoom_level >= Self::MIN_ZOOM_FOR_PLANET_TILES as i32 || keys_cache.contains(&key) {
                    // increase a bit bbox for tile to reduce borders artefacts.
                    // it's a good tradeoff between amount artefacts and over tesselation/drawing
                    // note: caching this calculation isn't helpful
                    
                    let tile_rect = key.calc_tile_boundary(1.01);

                    for item in tile_ranges.intersection(&map_geometry, &tile_rect, geom_rect) {
                        sender
                            .send((key, map_geom_object.clone(), item))
                            .unwrap();
                    }
                }
            }
        }
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

    fn perform_queries(tx: &Transaction, tile_db_map: &mut FxHashMap<TileKey, MapGeometryCollection>) {
        let mut stmt = tx.prepare("INSERT INTO tiles (x, y, z, data) VALUES (?1, ?2, ?3, ?4)")
            .unwrap();

        let len = tile_db_map.len();
        print!("Compressing: 0%");
        tile_db_map.iter_mut().enumerate().for_each(|(index, (key, data))| {
            data.0.sort_by(|(a, _), (b, _)| a.cmp(b));
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
