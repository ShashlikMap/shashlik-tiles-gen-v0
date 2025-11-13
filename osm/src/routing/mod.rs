use geo::{Coord, Distance, algorithm::line_measures::Euclidean};
use super::map::HighwayKind;
use serde::{Serialize, Deserialize};
use petgraph::graphmap::DiGraphMap;
use petgraph::algo::astar;
use std::collections::HashMap;
use thiserror::Error;
use error_stack::{Report, ResultExt};
use serde_bare::{from_reader, to_writer};
use std::fs::File;
use std::io::{BufReader, BufWriter};

#[derive(Debug, Clone, Copy, Error)]
pub enum RoutingError {
    #[error("Failed to load routing graph")]
    FailedToLoad,
    #[error("Failed to save routing graph")]
    FailedToSave
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct OsmRoadGraph {
    node_graph: DiGraphMap<i64, f32>,
    // TODO: make node ID -> Coord map shared across components to save memory
    node_map: HashMap<i64, Coord>
}

impl OsmRoadGraph {
    /// Add directional edge
    pub fn add_edge(&mut self, a_node: (i64, Coord), b_node: (i64, Coord), road_kind: HighwayKind) {

        self.node_graph.add_edge(
            a_node.0, 
            b_node.0, 
            (Euclidean::distance(a_node.1, b_node.1) * Self::highway_weight(road_kind)) as f32
        );

        self.node_map.entry(a_node.0).or_insert(a_node.1);
        self.node_map.entry(b_node.0).or_insert(b_node.1);
    }

    /// Add bi-directional edge
    pub fn add_bi_edge(&mut self, a_node: (i64, Coord), b_node: (i64, Coord), road_kind: HighwayKind) {
        let weight = (Euclidean::distance(a_node.1, b_node.1) * Self::highway_weight(road_kind)) as f32;

        self.node_graph.add_edge(a_node.0, b_node.0, weight);
        self.node_graph.add_edge(b_node.0, a_node.0, weight);

        self.node_map.entry(a_node.0).or_insert(a_node.1);
        self.node_map.entry(b_node.0).or_insert(b_node.1);
    }

    // TODO: move this into HighwayKind implementation
    fn highway_weight(kind: HighwayKind) -> f64 {
        match kind {
            // Large highways have double value over other types
            HighwayKind::Motorway
            | HighwayKind::MotorwayLink
            | HighwayKind::Primary
            | HighwayKind::PrimaryLink => 0.5,
            _ => 1.0
        }
    }

    /// Calculate set of coordinates to represent route from start to end location
    /// inputs are OSM road node IDs
    pub fn route(&self, from_id: i64, to_id: i64) -> Option<Vec<Coord>> {
        let destination_coord = self.node_map.get(&to_id)?;
        let route = astar(
            &self.node_graph, 
            from_id, 
            |n| n == to_id,
            |(_, _, e)| *e, 
            |nid|
                Euclidean::distance(
                    *self.node_map.get(&nid).expect("missing node"), 
                    *destination_coord
                ) as f32
        )?.1
            .into_iter()
            .map(|node| *self.node_map.get(&node).expect("missing node"))
            .collect();

        Some(route)
    }

    pub fn save(&self, path: &str) -> Result<(), Report<RoutingError>> {
        let writer = BufWriter::new(
            File::create(path).change_context(RoutingError::FailedToSave)?
        );
    
        to_writer(writer, self).change_context(RoutingError::FailedToSave)
    }

    pub fn load(path: &str) -> Result<Self, Report<RoutingError>> {
        let reader = BufReader::new(
            File::open(path).change_context(RoutingError::FailedToLoad)?
        );
    
        from_reader(reader).change_context(RoutingError::FailedToLoad)
    }
}

#[cfg(test)]
mod test {
    use geo::Coord;
    use super::{OsmRoadGraph, HighwayKind};

    #[test]
    fn test_routing() {
        let mut graph = OsmRoadGraph::default();

        graph.add_bi_edge((1, Coord { x: 0.1, y: 0.1 }), (2, Coord { x: 0.2, y: 0.2 }), HighwayKind::Primary);
        graph.add_edge((2, Coord { x: 0.2, y: 0.2 }), (3, Coord { x: 0.3, y: 0.3 }), HighwayKind::Primary);
        graph.add_edge((3, Coord { x: 0.3, y: 0.3 }), (4, Coord { x: 0.4, y: 0.4 }), HighwayKind::Primary);
        graph.add_bi_edge((2, Coord { x: 0.2, y: 0.2 }), (4, Coord { x: 0.4, y: 0.4 }), HighwayKind::Secondary);

        // Test getting from 1 to 4, mostly one directional roads
        let route_plan = graph.route(1, 4).unwrap();

        assert_eq!(
            route_plan,
            vec![
                Coord { x: 0.1, y: 0.1 },
                Coord { x: 0.2, y: 0.2 },
                Coord { x: 0.3, y: 0.3 },
                Coord { x: 0.4, y: 0.4 }
            ]
        );

        // Test getting from 4 to 1, using bi-directional route
        let route_plan = graph.route(4, 1).unwrap();

        assert_eq!(
            route_plan,
            vec![
                Coord { x: 0.4, y: 0.4 },
                Coord { x: 0.2, y: 0.2 },
                Coord { x: 0.1, y: 0.1 }
            ]
        );
    }
}