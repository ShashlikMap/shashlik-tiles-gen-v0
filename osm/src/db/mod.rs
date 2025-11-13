use crate::map::{MapGeomObject, MapGeometry};
use error_stack::{Report, ResultExt};
use rstar::{primitives::GeomWithData, Envelope, PointDistance, RTree, RTreeObject};
use serde::{de::DeserializeOwned, Serialize};
use serde_bare::{from_reader, to_writer};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use thiserror::Error;

#[derive(Clone, Error, Debug)]
pub enum OsmDbError {
    #[error("Failed to open")]
    Open,
    #[error("Failed to store")]
    Store,
    #[error("Failed to serialize")]
    Serialize,
    #[error("Failed to retrieve")]
    Retrieve,
    #[error("Failed to deserialize")]
    Deserialize,
}
pub type GeomIndex = RTree<GeomWithData<MapGeometry, MapGeomObject>>;

pub trait SpacialIndex<G, B, P, M>
where
    Self: Serialize + DeserializeOwned + Default,
{
    fn open(path: &str) -> Result<Self, Report<OsmDbError>> {
        let reader = BufReader::new(File::open(path).change_context(OsmDbError::Open)?);

        from_reader(reader).change_context(OsmDbError::Deserialize)
    }

    fn new() -> Self {
        Self::default()
    }

    fn save(&self, path: &str) -> Result<(), Report<OsmDbError>> {
        let writer = BufWriter::new(File::create(path).change_context(OsmDbError::Open)?);

        to_writer(writer, self).change_context(OsmDbError::Serialize)
    }

    fn db_insert(&mut self, metadata: M, obj: G);

    fn db_intersecting(&self, boundry: &B) -> impl Iterator<Item = (G, M)>;
}

impl<G, M> SpacialIndex<G, G::Envelope, <G::Envelope as Envelope>::Point, M> for RTree<GeomWithData<G, M>>
where
    G: RTreeObject + Serialize + DeserializeOwned + Clone + PointDistance,
    G::Envelope: Serialize + DeserializeOwned,
    M: Serialize + DeserializeOwned + Clone,
{
    fn db_insert(&mut self, metadata: M, obj: G) {
        self.insert(GeomWithData::new(obj, metadata))
    }

    fn db_intersecting(&self, boundry: &G::Envelope) -> impl Iterator<Item = (G, M)> {
        self.locate_in_envelope_intersecting(boundry)
            .map(|o| (o.geom().clone(), o.data.clone()))
    }
}
