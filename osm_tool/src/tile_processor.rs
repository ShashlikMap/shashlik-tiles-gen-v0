use crate::POLYGON_MERGE_ZOOM_LEVEL;
use geo::{Area, Polygon, Simplify};
use osm::map::MapGeomObjectKind::AdminLine;
use osm::map::NatureKind::Ground;
use osm::map::{MapGeomObject, MapGeomObjectKind, MapGeometry, MapPointObjectKind, NatureKind, ZOOM_LEVELS};
use osm::tile_writer::tile_writer::TileWriter;

pub struct TileProcessor {
    pub tile_writer: TileWriter,
}

impl TileProcessor {
    pub fn new() -> Self {
        TileProcessor {
            tile_writer: TileWriter::default(),
        }
    }

    pub fn add_to_tiles(
        &mut self,
        map_geom_object: MapGeomObject,
        map_geometry: MapGeometry,
    ) {
        match map_geom_object.kind {
            MapGeomObjectKind::Poi(..) => self.add_to_poi(map_geom_object, map_geometry),
            MapGeomObjectKind::Nature(..) => self.add_to_nature(map_geom_object, map_geometry),
            MapGeomObjectKind::AdminLine => self.add_to_nature(map_geom_object, map_geometry),
            MapGeomObjectKind::Building(..) => self.add_to_buildings(map_geom_object, map_geometry),
            _ => {}
        }
    }

    fn add_to_buildings(&mut self, map_geom_obj: MapGeomObject, geom: MapGeometry) {
        for zoom_level in 0..=1 {
            self.tile_writer.add_to_tiles(zoom_level, map_geom_obj.clone(), geom.clone(), true);
        }
    }

    // TODO Refactor to separate planet data from tiles data
    fn add_to_nature(&mut self, map_geom_obj: MapGeomObject, geom: MapGeometry) {
        let can_create_new_tiles = map_geom_obj.kind != AdminLine
            && map_geom_obj.kind != MapGeomObjectKind::Nature(Ground);

        // it's faster to simplify geometry that already simplified for previous zoom level
        let mut temp_geom = geom;
        for zoom_level in 0..ZOOM_LEVELS {
            if zoom_level >= POLYGON_MERGE_ZOOM_LEVEL
                && map_geom_obj.kind == MapGeomObjectKind::Nature(NatureKind::Forest)
            {
                break;
            }
            
            let zlf = zoom_level as f64;
            if let Some(geom) = match &temp_geom {
                MapGeometry::Line(ref line) => {
                    Some(MapGeometry::Line(line.simplify(0.001 * zlf)))
                }
                MapGeometry::Poly(ref poly) => {
                    let epsilon =
                        if map_geom_obj.kind == MapGeomObjectKind::Nature(Ground) {
                            0.00007
                        } else {
                            0.00003
                        };
                    let area = if map_geom_obj.kind == MapGeomObjectKind::Nature(Ground)
                    {
                        0.0001
                    } else {
                        0.0000003
                    };

                    let simplified_exterior = poly.exterior().simplify(epsilon * zlf * zlf);
                    let interiors = if zoom_level < 2 {
                        poly.interiors().into_iter().map(|line| line.simplify(epsilon * zlf * zlf)).collect()
                    } else {
                        Vec::new()
                    };
                    let np = Polygon::new(simplified_exterior, interiors);
                    // TODO Consider to calculate area for exterior only
                    if np.unsigned_area() < area * zlf * zlf {
                        // return immediately since all other zoom levels won't have data
                        return;
                    } else {
                        Some(MapGeometry::Poly(np))
                    }
                }
                _ => None,
            } {
                self.tile_writer.add_to_tiles(
                    zoom_level,
                    map_geom_obj.clone(),
                    geom.clone(),
                    can_create_new_tiles,
                );
                temp_geom = geom;
            };
        }
    }
    
    fn add_to_poi(&mut self, map_geom_obj: MapGeomObject, geom: MapGeometry) {
        for zoom_level in 0..ZOOM_LEVELS {
            match map_geom_obj.kind {
                MapGeomObjectKind::Poi(ref obj) => match obj.kind {
                    MapPointObjectKind::PopArea(info) => {
                        if info.level == 0 && zoom_level >= 5 && zoom_level <= 12
                            || info.level == 1 && zoom_level > 12
                        {
                            self.tile_writer.add_to_tiles(
                                zoom_level,
                                map_geom_obj.clone(),
                                geom.clone(),
                                false,
                            );
                        }
                    }
                    _ => {
                        let condition = match obj.kind {
                            MapPointObjectKind::TrafficLight => zoom_level == 0,
                            MapPointObjectKind::TrainStation(is_train) => {
                                zoom_level <= if is_train { 4 } else { 2 }
                            },
                            _ => zoom_level <= 1
                        };
                        if condition {
                            self.tile_writer.add_to_tiles(
                                zoom_level,
                                map_geom_obj.clone(),
                                geom.clone(),
                                true,
                            );
                        }
                    }
                },
                _ => {}
            }
        }
    }

    pub fn prepare_for_planet_data(&mut self) {
        self.tile_writer.flush_to_collections(true);
    }
    
    pub fn save_to_disk(&mut self) {
        self.tile_writer.save_to_file();
    }
}
