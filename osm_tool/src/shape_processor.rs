use crate::countries::TempCountries;
use crate::tile_processor::TileProcessor;
use geo::{coord, Area, BoundingRect, Intersects, Rect};
use osm::map::MapGeomObjectKind::{AdminLine, Poi};
use osm::map::NatureKind::Ground;
use osm::map::{
    MapGeomObject, MapGeomObjectKind, MapGeometry, MapPointInfo, MapPointObjectKind, PopAreaInfo,
};
use shapefile::dbase::FieldValue;
use shapefile::dbase::FieldValue::Character;
use std::fs::File;
use std::sync::mpsc::{channel, Sender};
use threadpool::ThreadPool;

pub struct ShapeProcessor {
    pub(crate) world_boundary: Rect,
}
impl ShapeProcessor {
    pub fn extract_planet_data(&self, tile_processor: &mut TileProcessor) {
        let thread_pool = ThreadPool::new(2);
        let (tx, rx) = channel::<(MapGeomObject, MapGeometry)>();
        Self::extract_countries_and_cities(&thread_pool, tx.clone());
        Self::extract_land_shapes(&thread_pool, tx.clone(), self.world_boundary);
        Self::extract_admin_boundaries(&thread_pool, tx, self.world_boundary);

        tile_processor.prepare_for_planet_data();

        for item in rx {
            let (map_geom_obj, geom) = item;
            tile_processor.add_to_tiles(map_geom_obj, geom);
        }
    }

    fn extract_land_shapes(
        thread_pool: &ThreadPool,
        sender: Sender<(MapGeomObject, MapGeometry)>,
        world_boundary: Rect,
    ) {
        println!("Extract land shapes");
        thread_pool.execute(move || {
            let shapes = shapefile::read_shapes_as::<_, shapefile::Polygon>(
                "./land_shapes/land_polygons.shp",
            )
            .unwrap();
            let mut shapes_amount = 0;
            shapes
                .into_iter()
                .filter_map(|item| {
                    let mpoly: geo::MultiPolygon = item.into();
                    if !world_boundary.intersects(&mpoly.bounding_rect().unwrap()) {
                        return None;
                    }
                    // MultiPolygon for land should have only one polygon
                    let poly = mpoly.0.first().unwrap();
                    // there are around 800000 shapes, we're still not really interested in all of them
                    if poly.unsigned_area() < 0.001 {
                        return None;
                    }
                    Some(poly.clone())
                })
                .for_each(|item| {
                    shapes_amount += 1;
                    sender
                        .send((
                            MapGeomObject {
                                id: -1,
                                kind: MapGeomObjectKind::Nature(Ground),
                            },
                            MapGeometry::Poly(item),
                        ))
                        .unwrap();
                });
            print!("\rLand shapes extracted, count: {}\n", shapes_amount);
        });
    }

    fn extract_countries_and_cities(
        thread_pool: &ThreadPool,
        sender: Sender<(MapGeomObject, MapGeometry)>,
    ) {
        thread_pool.execute(move || {
            // TODO Find shapefile for that
            let temp_countries: TempCountries =
                serde_json::from_reader(File::open("temp_countries.json").unwrap())
                    .expect("JSON was not well-formatted");
            temp_countries.ref_country_codes.iter().for_each(|country| {
                let coord = coord! {x: country.longitude, y: country.latitude};

                let map_geom_obj = MapGeomObject {
                    id: -1, // what to do with ID here?,
                    kind: Poi(MapPointInfo {
                        text: country.country.to_string(),
                        kind: MapPointObjectKind::PopArea(PopAreaInfo {
                            level: 1,
                            population: 0,
                        }),
                    }),
                };
                sender.send((map_geom_obj, MapGeometry::Coord(coord))).unwrap();
            });
            // can be downloaded from https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/50m/cultural/ne_50m_populated_places.zip
            match shapefile::read("./ne_50m_populated_places/ne_50m_populated_places.shp") {
                Ok(cities) => {
                    for (_, record) in &cities {
                        let name = match record.get("NAME").unwrap() {
                            Character(f) => f.clone().unwrap(),
                            _ => "".to_string(),
                        };
                        let population = match record.get("POP_MIN").unwrap() {
                            FieldValue::Numeric(f) => f.unwrap() as u32,
                            _ => 0,
                        };
                        let lon = match record.get("LONGITUDE").unwrap() {
                            FieldValue::Numeric(f) => f.unwrap(),
                            _ => 0.0,
                        };
                        let lat = match record.get("LATITUDE").unwrap() {
                            FieldValue::Numeric(f) => f.unwrap(),
                            _ => 0.0,
                        };
                        if !name.is_empty() {
                            let coord = coord! {x: lon, y: lat};
                            let map_geom_obj = MapGeomObject {
                                id: -1, // what to do with ID here?,
                                kind: MapGeomObjectKind::Poi(MapPointInfo {
                                    text: name,
                                    kind: MapPointObjectKind::PopArea(PopAreaInfo {
                                        level: 0,
                                        population,
                                    }),
                                }),
                            };
                            sender.send((map_geom_obj, MapGeometry::Coord(coord))).unwrap();
                        }
                    }
                }
                Err(_) => {
                    println!("Can't read cities shapefile");
                }
            }
            println!("Countries and cities extracted");
        });
    }

    fn extract_admin_boundaries(
        thread_pool: &ThreadPool,
        sender: Sender<(MapGeomObject, MapGeometry)>,
        world_boundary: Rect,
    ) {
        println!("Extract admin boundaries");
        thread_pool.execute(move || {
            let mut shapes_amount = 0;
            match shapefile::read_shapes_as::<_, shapefile::Polyline>(
                "./ne_50m_admin_0_boundary_lines_land/ne_50m_admin_0_boundary_lines_land.shp",
            ) {
                Ok(shapes) => {
                    shapes
                        .into_iter()
                        .filter_map(|item| {
                            let line: geo::MultiLineString = item.into();
                            if !world_boundary.intersects(&line.bounding_rect().unwrap()) {
                                return None;
                            }
                            Some(line.0.first().unwrap().clone())
                        })
                        .for_each(|item| {
                            shapes_amount += 1;
                            sender
                                .send((
                                    MapGeomObject {
                                        id: -1,
                                        kind: AdminLine,
                                    },
                                    MapGeometry::Line(item),
                                ))
                                .unwrap();
                        });
                }
                Err(e) => {
                    println!("Error extracting admin lines {:?}", e);
                }
            }
            print!("\rAdmin lines extracted, count: {}\n", shapes_amount);
        });
    }
}
