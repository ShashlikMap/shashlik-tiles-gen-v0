use crate::filter;
use crate::proto::{Blob, BlobHeader, PrimitiveBlock, Relation};
use crate::tags::IntoTagIterator;
use error_stack::{Report, ResultExt};
use geo::{Coord, Intersects, LineString, Polygon, Rect};
use itertools::izip;
use prost::Message;
use rustc_hash::{FxHashMap, FxHashSet};
use std::io::{Seek, SeekFrom};
use std::iter::Iterator;
use std::{
    collections::HashMap,
    io::{ErrorKind, Read},
};

/// Packed delta value decoder
struct Delta<I> {
    acu: Option<i64>,
    iter: I,
}

impl<I> Delta<I> {
    pub fn new(iter: I) -> Self {
        Delta { acu: None, iter }
    }
}

impl<I: Iterator<Item = i64>> Iterator for Delta<I> {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| match &mut self.acu {
            Some(acu) => {
                *acu += v;

                *acu
            }
            None => {
                self.acu = Some(v);

                v
            }
        })
    }
}

trait IntoDelta: Sized {
    fn delta(self) -> Delta<Self>;
}

impl<I: Iterator<Item = i64>> IntoDelta for I {
    fn delta(self) -> Delta<Self> {
        Delta::new(self)
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub enum OsmBlobReaderError {
    #[error("Failed to read OSM blob")]
    Read,
    #[error("Failed to decode OSM blob")]
    Decode,
    #[error("Unsupported blob type {0}")]
    UnsupportedType(String),
}

#[derive(Debug, Clone)]
pub struct OsmWay {
    pub id: i64,
    pub tags: HashMap<u32, u32>,
    pub refs: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct OsmNode {
    pub id: i64,
    pub coord: Coord,
    pub tags: HashMap<u32, u32>,
}

impl OsmWay {
    pub fn as_line(&self, nodes: &FxHashMap<i64, Coord>) -> (LineString, i64, i64) {
        let mut f_node: i64 = -1;
        let mut l_node: i64 = -1;
        (
            self.refs
                .iter()
                .filter_map(|i| {
                    nodes.get(i).map(|v| {
                        if f_node == -1 {
                            f_node = *i;
                        }
                        l_node = *i;
                        v.clone()
                    })
                })
                .collect(),
            f_node,
            l_node,
        )
    }

    pub fn as_polygon(&self, nodes: &FxHashMap<i64, Coord>) -> Polygon {
        Polygon::new(self.as_line(nodes).0, vec![])
    }
}

pub struct OsmRelation {
    pub id: i64,
    pub tags: HashMap<u32, u32>,
    pub ways: Vec<(i64, i32)>, // way id + role id
}

impl OsmRelation {
    pub fn new(relation: Relation) -> Self {
        let tags = izip!(relation.keys.into_iter(), relation.vals.into_iter()).collect();

        let ways = izip!(
            relation.types.into_iter(),
            relation.roles_sid.into_iter(),
            relation.memids.into_iter().delta(),
        )
        .filter_map(|(t, role, i)| if t == 1 { Some((i, role)) } else { None })
        .collect();

        Self {
            id: relation.id,
            tags,
            ways,
        }
    }
}

pub struct OsmBlobData {
    pub string_table: Vec<String>,
    pub ways: Vec<OsmWay>,
    pub nodes: Vec<OsmNode>,
    pub relations: Vec<OsmRelation>,
}

impl std::fmt::Debug for OsmBlobData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OsmBlobData {{ string_table_size: {}; ways_count: {} }}",
            self.string_table.len(),
            self.ways.len()
        )
    }
}

#[derive(Debug)]
pub struct OsmBlobHeader {
    pub blob_header: BlobHeader,
    pub total_size: i32,
}

#[derive(Debug)]
pub struct OsmHeaderBlock {
    // Nothing of interest for now
}

#[derive(Debug)]
pub enum OsmBlob {
    Data(OsmBlobData),
    Header(OsmHeaderBlock),
}

pub struct OsmReader<T> {
    input: T,
    header_len_buffer: [u8; 4],
    header_buffer: Vec<u8>,
    blob_buffer: Vec<u8>,
    boundry: Rect,
}

impl<T: Read + Seek> OsmReader<T> {
    pub fn new(input: T, boundry: Rect) -> Self {
        Self {
            input,
            header_len_buffer: [0; 4],
            header_buffer: Vec::new(),
            blob_buffer: Vec::new(),
            boundry,
        }
    }

    /// Pre extract way id which part of Relations. It reduces the memory consumption and speed up the process since
    /// there will be fewer ways in cache in general, we cache only what we need for Relations.
    pub fn extract_ways_id_from_relations(
        &mut self,
        relation_tags: &[(&str, Option<&str>)],
    ) -> FxHashSet<i64> {
        let mut blocks_sizes = Vec::new();
        let mut total_size = 0u64;
        loop {
            let osm_blob_header = match self.parse_blob_header() {
                Ok(data) => match data {
                    None => break,
                    Some(data) => data,
                },
                Err(_) => break,
            };
            let block_size = osm_blob_header.total_size as u64;
            total_size += block_size;
            blocks_sizes.push(block_size);
            // skip the blob itself
            self.input
                .seek(SeekFrom::Current(
                    osm_blob_header.blob_header.datasize as i64,
                ))
                .unwrap();
        }

        let mut ways_ids = FxHashSet::default();
        // the PBF order is Nodes/DenseNodes - Ways - Relations.
        // we just start from the end until there are no relations
        let mut last_block_index = blocks_sizes.len() as i32 - 1;
        while last_block_index >= 0 {
            total_size -= blocks_sizes[last_block_index as usize];
            last_block_index -= 1;
            self.input.seek(SeekFrom::Start(total_size as u64)).unwrap();
            let blob = self.parse_blob().unwrap().ok().unwrap();
            if let OsmBlob::Data(data) = blob {
                if !data.nodes.is_empty() || !data.ways.is_empty() {
                    break;
                }
                let tag_filter = filter::TagFilter::new(&data.string_table, relation_tags);
                let rels = data.relations;
                for rel in rels {
                    // count only Relations that will be parsed later
                    // TODO This should be the only place where we parse Relation
                    //  but Relation parsing takes relatively small amount of time so it's not a
                    //  priority now
                    if let Some((_, _)) = tag_filter.filter(&data.string_table, &rel.tags) {
                        rel.ways.iter().for_each(|way| {
                            let way_id = way.0;
                            ways_ids.insert(way_id);
                        })
                    }
                }
            } else {
                break;
            }
        }
        // seek to the beginning so Iterator would start from the beginning
        self.input.seek(SeekFrom::Start(0)).unwrap();
        println!("Extracted ways from all relations: {}", ways_ids.len());
        ways_ids
    }

    fn parse_blob_header(&mut self) -> Result<Option<OsmBlobHeader>, Report<OsmBlobReaderError>> {
        if let Err(err) = self.input.read_exact(&mut self.header_len_buffer) {
            if err.kind() == ErrorKind::UnexpectedEof {
                return Ok(None);
            }

            return Err(err).change_context(OsmBlobReaderError::Read);
        }

        let header_len_buffer_size = i32::from_be_bytes(self.header_len_buffer);
        self.header_buffer
            .resize(header_len_buffer_size as usize, 0);

        if let Err(err) = self.input.read_exact(self.header_buffer.as_mut()) {
            return Err(err).change_context(OsmBlobReaderError::Read);
        }

        match BlobHeader::decode(self.header_buffer.as_slice()) {
            Ok(header) => {
                let header_datasize = header.datasize;
                Ok(Some(OsmBlobHeader {
                    blob_header: header,
                    total_size: (size_of::<u8>() * 4) as i32
                        + header_len_buffer_size
                        + header_datasize,
                }))
            }
            Err(err) => Err(err).change_context(OsmBlobReaderError::Decode),
        }
    }

    fn parse_blob(&mut self) -> Option<Result<OsmBlob, Report<OsmBlobReaderError>>> {
        let blob = match self.read_blob().unwrap() {
            Ok(blob) => blob,
            Err(err) => return Some(Err(err)),
        };

        Self::blob_to_osm_blob_data(blob, self.boundry)
            .map(|osm_blob_data| Ok(OsmBlob::Data(osm_blob_data)))
    }

    pub fn read_blob(&mut self) -> Option<Result<Blob, Report<OsmBlobReaderError>>> {
        let blob_header = match self.parse_blob_header() {
            Ok(header) => header,
            Err(err) => return Some(Err(err).change_context(OsmBlobReaderError::Decode)),
        }?
        .blob_header;

        self.blob_buffer.resize(blob_header.datasize as usize, 0);

        if let Err(err) = self.input.read_exact(&mut self.blob_buffer) {
            if err.kind() == ErrorKind::UnexpectedEof {
                return None;
            }

            return Some(Err(err).change_context(OsmBlobReaderError::Read));
        }

        let blob = match Blob::decode(self.blob_buffer.as_slice()) {
            Ok(blob) => blob,
            Err(err) => return Some(Err(err).change_context(OsmBlobReaderError::Decode)),
        };
        Some(Ok(blob))
    }

    fn blob_to_osm_blob_data(blob: Blob, boundary: Rect) -> Option<OsmBlobData> {
        let deflated_blob = match blob.extract().change_context(OsmBlobReaderError::Decode) {
            Err(_) => return None,
            Ok(deflated) => deflated,
        };

        let primitive =
            PrimitiveBlock::decode(deflated_blob).change_context(OsmBlobReaderError::Decode);

        let primitive = match primitive {
            Err(_) => return None,
            Ok(primitive) => primitive,
        };

        let string_table: Vec<String> = primitive
            .stringtable
            .s
            .into_iter()
            .map(|s| String::from_utf8_lossy(s.as_slice()).to_string())
            .collect();

        let lat_offset = primitive.lat_offset.unwrap_or_default();
        let lon_offset = primitive.lon_offset.unwrap_or_default();
        let granularity = primitive.granularity.unwrap_or(100);
        let mut nodes = Vec::new();
        let mut ways: Vec<OsmWay> = Vec::new();
        let mut relations: Vec<OsmRelation> = Vec::new();

        for pg in primitive.primitivegroup {
            if let Some(dn) = pg.dense {
                let id_coord = izip!(
                    dn.id.into_iter().delta(),
                    dn.lat.into_iter().delta(),
                    dn.lon.into_iter().delta(),
                    dn.keys_vals.into_iter().tags()
                )
                .map(|(id, lat, lon, tags)| OsmNode {
                    id: id,
                    coord: Coord {
                        x: 0.000000001
                            * ((lon as i128 * granularity as i128) + lon_offset as i128) as f64,
                        y: 0.000000001
                            * ((lat as i128 * granularity as i128) + lat_offset as i128) as f64,
                    },
                    tags,
                })
                .filter(|osm_node| boundary.intersects(&osm_node.coord));

                nodes.extend(id_coord);
            }

            let ns = pg
                .nodes
                .into_iter()
                .map(|n| OsmNode {
                    id: n.id,
                    coord: Coord {
                        x: 0.000000001
                            * ((n.lon as i128 * granularity as i128) + lon_offset as i128) as f64,
                        y: 0.000000001
                            * ((n.lat as i128 * granularity as i128) + lat_offset as i128) as f64,
                    },
                    tags: izip!(n.keys.into_iter(), n.vals.into_iter()).collect(),
                })
                .filter(|osm_node| boundary.intersects(&osm_node.coord));

            nodes.extend(ns);

            let ws = pg.ways.into_iter().map(|w| OsmWay {
                id: w.id,
                tags: izip!(w.keys.into_iter(), w.vals.into_iter()).collect(),
                refs: w.refs.into_iter().delta().collect(),
            });

            ways.extend(ws);

            relations.extend(pg.relations.into_iter().map(OsmRelation::new));
        }

        let data = OsmBlobData {
            string_table,
            ways,
            nodes,
            relations,
        };
        Some(data)
    }

    pub fn data(&mut self) -> (Vec<OsmBlobData>, Vec<OsmBlobData>, Vec<OsmBlobData>) {
        let (tx, rx) = std::sync::mpsc::channel::<OsmBlobData>();
        let tp = threadpool::ThreadPool::new(6);

        let boundary = self.boundry;
        while let Some(blob) = self.read_blob() {
            let blob = blob.expect("Failed to read blob");
            let sender = tx.clone();
            tp.execute(move || {
                if let Some(data) = Self::blob_to_osm_blob_data(blob, boundary) {
                    sender.send(data).unwrap();
                }
            });
        }
        println!("Reading finished");
        drop(tx);
        
        // TODO It might be optimized by preprocessing blocks information before multithreading
        let mut node_blobs = Vec::new();
        let mut way_blobs = Vec::new();
        let mut rels_blobs = Vec::new();
        for data_blob in rx {
            if data_blob.nodes.len() > 0 {
                node_blobs.push(data_blob);
            } else if data_blob.ways.len() > 0 {
                way_blobs.push(data_blob);
            } else {
                rels_blobs.push(data_blob);
            }
        }

        (node_blobs, way_blobs, rels_blobs)
    }
}

impl<T: Read + Seek> Iterator for OsmReader<T> {
    type Item = Result<OsmBlob, Report<OsmBlobReaderError>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.parse_blob()
    }
}
