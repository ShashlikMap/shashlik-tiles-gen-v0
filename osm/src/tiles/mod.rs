mod sutherland_hodgman;

use crate::map::{get_world_boundary, MapGeomObject, MapGeometry, MapGeometryCollection};
use geo::line_intersection::line_intersection;
use geo::{coord, Contains, Coord, Intersects, Line, LineIntersection, LineString, MultiLineString, Polygon, Rect, Scale};
use itertools::Itertools;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

pub const TILES_COUNT: i32 = 32768;

const TILES_SELECT_QUERY: &str = "SELECT data FROM tiles WHERE x=:x AND y=:y AND z=:z;";

#[derive(Hash, PartialEq, Eq, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TileKey {
    pub tile_x: i32,
    pub tile_y: i32,
    pub zoom_level: i32,
}

impl TileKey {
    pub fn calc_tile_boundary(&self, scale_factor: f64) -> Rect {
        let world_rect = get_world_boundary();

        let tiles_count = (TILES_COUNT / 2i32.pow(self.zoom_level as u32)).max(1);
        let tile_width = world_rect.width() / tiles_count as f64;
        let tile_height = world_rect.height() / tiles_count as f64;
        let p1 = coord!(x: tile_width * self.tile_x as f64 + world_rect.min().x,
            y: tile_height * self.tile_y as f64 + world_rect.min().y);
        let p2 = coord!(x: p1.x + tile_width, y: p1.y + tile_height);
        Rect::new(p1, p2).scale(scale_factor)
    }
}

#[derive(Clone)]
pub struct TileRanges {
    pub min_x: u32,
    pub max_x: u32,
    pub min_y: u32,
    pub max_y: u32,
}

impl TileRanges {
    pub(crate) fn intersection(&self, map_geometry: &MapGeometry, tile_rect: &Rect, geom_rect: &Rect) -> Vec<MapGeometry> {
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
                        if !tile_rect.contains(&line.start) && !tile_rect.contains(&line.end) && self.rect_intersection(tile_rect, &line).is_none() {
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

                if let Some(intersected) = sutherland_hodgman::sutherland_hodgman_clip(exterior, tile_rect) {
                    let intersected_inters = poly.interiors().iter().flat_map(|interior| {
                        sutherland_hodgman::sutherland_hodgman_clip(interior, tile_rect)
                    }).collect_vec();
                    vec![MapGeometry::Poly(Polygon::new(intersected, intersected_inters))]
                } else {
                    vec![]
                }
                
                // TODO make boolean intersection available with config a bit later
                // let intersected = poly.clone().intersection(&rect.to_polygon());
                // intersected.0.iter().map(|item| MapGeometry::Poly(item.clone())).collect()
            }
            MapGeometry::Coord(_) => vec![map_geometry.clone()]
        }
    }

    fn rect_intersection(&self, rect: &Rect, line: &Line) -> Option<Coord> {
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
}

pub fn calc_tile_ranges(total_tiles: i32, zoom_level: i32, rect: &Rect) -> TileRanges {
    let world_rect = get_world_boundary();

    let tiles_count = (total_tiles / 2i32.pow(zoom_level as u32)).max(1);
    let tiles_count_f64 = tiles_count as f64;

    let tile_min_x = ((tiles_count_f64
        * ((rect.min().x - world_rect.min().x)
        / world_rect.width())) as i32)
        .clamp(0, tiles_count - 1);
    let tile_max_x = ((tiles_count_f64
        * ((rect.max().x - world_rect.min().x)
        / world_rect.width())) as i32)
        .clamp(0, tiles_count - 1);
    let tile_min_y = ((tiles_count_f64
        * ((rect.min().y - world_rect.min().y)
        / world_rect.height())) as i32)
        .clamp(0, tiles_count - 1);
    let tile_max_y = ((tiles_count_f64
        * ((rect.max().y - world_rect.min().y)
        / world_rect.height())) as i32)
        .clamp(0, tiles_count - 1);

    TileRanges {
        min_x: tile_min_x as u32,
        max_x: tile_max_x as u32,
        min_y: tile_min_y as u32,
        max_y: tile_max_y as u32,
    }
}

pub fn closest_power_2(zoom: i32) -> i32 {
    if zoom < 2 {
        return zoom;
    }
    let mut t = 1;
    let mut a = 0;
    // TODO change to log
    while t * 2 <= zoom {
        t *= 2;
        a += 1;
    }
    a + 1
}

pub fn create_tiles_db_connection() -> Connection {
    Connection::open("dbs/tiles.db").unwrap()
}

impl TileKey {
    pub fn new(tile_x: i32, tile_y: i32, zoom_level: i32) -> Self {
        TileKey {
            tile_x,
            tile_y,
            zoom_level,
        }
    }
}

pub struct TileStore {
    db_conn: Mutex<Connection>,
}

impl TileStore {
    pub fn new() -> TileStore {
        Self {
            db_conn: Mutex::new(create_tiles_db_connection()),
        }
    }

    pub fn load_geometries(&self,
                           tile_key: &TileKey) -> Vec<(MapGeomObject, MapGeometry)> {
        let conn = self.db_conn.lock().unwrap();
        let mut stmt = conn.prepare(TILES_SELECT_QUERY).unwrap();
        // TODO Refactor
        let data_iter = stmt.query_map(&[(":x", tile_key.tile_x.to_string().as_str()),
            (":y", tile_key.tile_y.to_string().as_str()),
            (":z", tile_key.zoom_level.to_string().as_str())], |row| {
            let data: Vec<u8> = row.get(0).unwrap();
            let collection: MapGeometryCollection = bincode::deserialize(&data).unwrap();
            Ok(collection)
        }).unwrap();
        let mut res = Vec::new();
        for item in data_iter {
            match item {
                Ok(collection) => {
                    res.extend(collection.0)
                }
                Err(error) => {
                    println!("Error: {:?}", error);
                }
            }
        }
        res
    }

}

impl Default for TileStore {
    fn default() -> Self {
        TileStore::new()
    }
}