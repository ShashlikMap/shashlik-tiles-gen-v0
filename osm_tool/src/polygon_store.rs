use crate::{LocationTraitCoord, POLYGON_MERGE_ZOOM_LEVEL};
use geo::{
    coord, Area, BooleanOps, Coord, CoordsIter, Intersects, LineString, Polygon, Scale, SimplifyVw,
};
use itertools::Itertools;
use osm::map::{MapGeomObject, MapGeomObjectKind, MapGeometry, NatureKind, ZOOM_LEVELS};
use rstar::{RTree, RTreeObject};
use std::io;
use std::io::Write;
use std::sync::mpsc::Sender;

pub struct PolygonStore {
    items: Vec<Polygon>,
}

impl PolygonStore {
    pub fn new() -> Self {
        PolygonStore {
            items: Vec::new(),
        }
    }
    
    pub fn add_polygon(&mut self, polygon: Polygon) {
        self.items.push(polygon);
    }

    pub fn process_forests_async(
        &self,
        sender: Sender<(u32, MapGeomObject, MapGeometry)>,
        merge_enabled: bool,
        zoom_level: u32,
    ) {
        let forest_polygons = self.items.clone();
        std::thread::spawn(move || {
            Self::process_forests(sender, merge_enabled, forest_polygons, zoom_level);
        });
    }

    fn process_forests(
        sender: Sender<(u32, MapGeomObject, MapGeometry)>,
        merge_enabled: bool,
        forest_polygons: Vec<Polygon>,
        zoom_level: u32,
    ) {
        let total_polygon_nodes: i32 = forest_polygons
            .iter()
            .map(|item| item.coords_count() as i32)
            .sum();
        println!(
            "Process forests for zoom level = {:?}, len = {:?}, nodes = {}",
            zoom_level,
            forest_polygons.len(),
            total_polygon_nodes
        );
        let zoom_level = zoom_level.max(POLYGON_MERGE_ZOOM_LEVEL).min(ZOOM_LEVELS);
        let zlf = zoom_level as f64;

        let forest_polygons = if merge_enabled {
            let merged_polygons = Self::merge_polygons(forest_polygons);
            let forest_polygons = merged_polygons
                .0
                .into_iter()
                .filter(|item| item.unsigned_area() >= 0.00000005 * (zlf - 2.0) * (zlf - 2.0))
                .collect_vec();

            let total_polygon_nodes: i32 = forest_polygons
                .iter()
                .map(|item| item.coords_count() as i32)
                .sum();
            print!(
                "\rMerge finished!, len = {}, nodes = {}\n",
                forest_polygons.len(),
                total_polygon_nodes
            );

            let forest_polygons_len = forest_polygons.len();
            let mut rtree: RTree<Polygon> = RTree::new();
            for (index, poly) in forest_polygons.into_iter().enumerate() {
                print!(
                    "\rAggregation: {}%",
                    (100.0 * index as f32 / forest_polygons_len as f32) as i32
                );
                let drained = rtree
                    .drain_in_envelope_intersecting(poly.scale(1.5).envelope())
                    .collect_vec();

                let mut coords_for_concavehull = Vec::new();

                for geom_poly in drained {
                    if geom_poly.unsigned_area()
                        > 0.000005 * (zlf - 2.0) * (zlf - 2.0) * (zlf - 2.0)
                    {
                        rtree.insert(geom_poly);
                    } else {
                        let scale_koef = 1.01 + 0.03 * (zlf - 2.0);
                        let test_poly1 = poly.scale(scale_koef);
                        let test_poly2 = geom_poly.scale(scale_koef);
                        if test_poly1.intersects(&test_poly2) {
                            coords_for_concavehull.extend(geom_poly.coords_iter().collect_vec());
                        } else {
                            rtree.insert(geom_poly);
                        }
                    }
                }

                let geom = if coords_for_concavehull.len() > 0 {
                    let densified = Self::densify_twice(&poly);
                    coords_for_concavehull.extend(densified);

                    let coords_vec = coords_for_concavehull
                        .iter()
                        .map(|coord| LocationTraitCoord { coord: *coord })
                        .collect_vec();
                    let concave_hull = rs_concaveman::concaveman(coords_vec.as_slice(), None, None)
                        .iter()
                        .map(|item| Coord {
                            x: item.0,
                            y: item.1,
                        })
                        .collect_vec();

                    Polygon::new(LineString(concave_hull), vec![])
                } else {
                    poly
                };

                rtree.insert(geom);
            }

            print!("\rAggregate finished, len = {}\n", rtree.size());

            rtree.drain().collect_vec()
        } else {
            forest_polygons
        };

        let all_geom = forest_polygons
            .iter()
            .filter_map(|poly| {
                if poly.unsigned_area() < 0.000003 * (zlf - 2.0) * (zlf - 2.0) {
                    None
                } else {
                    Some(poly.simplify_vw(0.0000003 * (zlf - 2.0) * (zlf - 2.0)))
                }
            })
            .collect_vec();

        all_geom.iter().for_each(|geom| {
            sender
                .send((
                    zoom_level,
                    MapGeomObject {
                        id: -2, // TODO figure out what to do with merged IDs
                        kind: MapGeomObjectKind::Nature(NatureKind::Forest),
                    },
                    MapGeometry::Poly(geom.clone()),
                ))
                .unwrap();
        });

        if zoom_level + 1 < ZOOM_LEVELS {
            Self::process_forests(sender, merge_enabled, all_geom, zoom_level + 1);
        }
    }

    fn densify_twice(poly: &Polygon) -> Vec<Coord> {
        let mut densified_exterior = Vec::new();
        poly.exterior().lines().for_each(|line| {
            if densified_exterior.len() == 0 {
                densified_exterior.push(line.start);
            }
            let middle = coord! { x: line.start.x + (line.end.x - line.start.x) / 2.0,
            y: line.start.y + (line.end.y - line.start.y) / 2.0 };
            densified_exterior.push(middle);
            densified_exterior.push(line.end);
        });
        poly.interiors().iter().for_each(|interior| {
            densified_exterior.extend(interior.coords());
        });
        densified_exterior
    }

    fn merge_polygons(polygons: Vec<Polygon>) -> geo::MultiPolygon {
        let mut polygons = polygons
            .into_iter()
            .map(|item| geo::MultiPolygon::new(vec![item]))
            .collect_vec();
        if polygons.len() == 0 {
            return geo::MultiPolygon::new(vec![]);
        }
        if polygons.len() == 1 {
            return polygons.first().unwrap().simplify_vw(0.00000001);
        }

        // look https://github.com/boostorg/geometry/discussions/947 for details
        let mut step = 1;
        let mut half_step = 0;
        loop {
            half_step = step;
            step *= 2;
            let mut i = 0;

            loop {
                let p1 = polygons.get(i).unwrap();
                let p2 = polygons.get(i + half_step).unwrap();

                polygons[i] = p1.union(p2);

                i += step;
                if i + half_step >= polygons.len() {
                    break;
                }
            }

            if step >= polygons.len() {
                break;
            }
            print!(
                "\rMerging: {}%",
                (100.0 * step as f32 / polygons.len() as f32) as i32
            );
            io::stdout().flush().unwrap();
        }
        polygons.first().unwrap().simplify_vw(0.00000001)
    }
}
