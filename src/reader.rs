use ahash::RandomState;
use anyhow::Result;
use bytes::Bytes;
use deku::{bitvec::BitView, DekuRead};
use std::{collections::HashMap, io::SeekFrom, sync::Arc};
use tracing::debug;

use serde_json::Value;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, BufReader},
    sync::RwLock,
};

use crate::{
    header::HEADER_BYTES,
    util::{decompress_async, read_dir_rec_async, tile_id, OffsetLength},
    Compression, Header,
};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct PMTilesReader<R, S> {
    reader: Arc<RwLock<R>>,
    pub tile_reader: Option<S>,
    pub header: Header,
    pub metadata: Option<Value>,
    pub tiles: HashMap<u64, OffsetLength, RandomState>,
}

impl<R, S> PMTilesReader<R, S>
where
    R: AsyncRead + AsyncSeek + Send + Unpin + Sync + Clone,
    S: PMTilesReadSeek + Send + Sync,
{
    /// Create a new `PMTilesReader` from a reader
    ///
    /// # Errors
    /// Will return an error if the reader is not valid `PMTiles`.
    pub async fn new(mut reader: R, tile_reader: Option<S>) -> Result<Self> {
        // The header always needs to be parsed
        let mut header_chunk = [0; HEADER_BYTES as usize];
        reader.seek(SeekFrom::Start(0)).await?;
        reader.read_exact(&mut header_chunk).await?;

        let (_, pmtiles_header) = Header::read(header_chunk.to_vec().view_bits(), ())?;

        // Load metadata
        let metadata = if pmtiles_header.json_metadata_length == 0 {
            None
        } else {
            {
                // Seek to metadata offset
                reader
                    .seek(SeekFrom::Start(pmtiles_header.json_metadata_offset))
                    .await?;

                // Prepare a buffer
                let mut metadata_bytes =
                    vec![0u8; usize::try_from(pmtiles_header.json_metadata_length)?];

                // Read into buffer
                reader.read_exact(&mut metadata_bytes).await?;

                // Create as a buffer
                let metadata_reader = BufReader::new(metadata_bytes.as_slice());

                // Decompress if needed
                let val =
                    parse_metadata(pmtiles_header.internal_compression, metadata_reader).await?;

                Some(val)
            }
        };

        // Prepare to hold tiles
        let mut tiles = HashMap::<u64, OffsetLength, RandomState>::default();

        read_dir_rec_async(
            &mut reader,
            &mut tiles,
            pmtiles_header.internal_compression,
            (
                pmtiles_header.root_directory_offset,
                pmtiles_header.root_directory_length,
            ),
            pmtiles_header.leaf_directories_offset,
            &(..),
        )
        .await?;

        Ok(Self {
            reader: Arc::new(RwLock::new(reader)),
            header: pmtiles_header,
            metadata,
            tiles,
            tile_reader,
        })
    }

    // pub async fn new_without_tile_reader(reader: R) -> Result<Self> {
    //     let tile_reader = PMTilesReadSeeker::new(reader.clone());
    //     Self::new(reader.clone(), Some(tile_reader)).await
    // }

    pub fn num_tiles(&self) -> usize {
        self.tiles.len()
    }

    /// Recursively locates a tile in the archive.
    fn find_tile_entry(&self, tile_id: u64) -> Option<OffsetLength> {
        // if let Some(entry) = self.tiles.get(&tile_id) {
        //     if entry.is_leaf() {
        //         return self.find_entry_rec(tile_id, entry, 0).await;
        //     }
        // }
        // entry.cloned()

        self.tiles.get(&tile_id).copied()
    }

    /// Fetches tile bytes from the archive.
    pub async fn get_tile(&self, z: u8, x: u64, y: u64) -> Option<Bytes> {
        let tile_id = tile_id(z, x, y);
        let entry = self.find_tile_entry(tile_id)?;

        debug!(
            "z/x/y: {}/{}/{}, tile_id: {}, entry: {:?}",
            z, x, y, tile_id, entry
        );

        let offset = self.header.tile_data_offset + entry.offset;
        let length = entry.length;

        let mut buf: Vec<u8> = vec![0; length.try_into().unwrap_or(0)];

        // Prefer using a reader that just goes to an offset and gets bytes
        if let Some(sane_reader) = &self.tile_reader {
            let bytes = sane_reader
                .get_bytes_at_offset(
                    length.try_into().unwrap_or_default(),
                    offset.try_into().unwrap_or_default(),
                )
                .await
                .ok();

            return bytes;
        }

        let mut reader = self.reader.write().await;
        reader.seek(SeekFrom::Start(offset)).await.ok()?;
        reader.read_exact(&mut buf).await.ok()?;
        drop(reader);

        Some(Bytes::from(buf))
    }
}

/// Parse the metadata from the metadata reader
///
/// # Errors
/// Will return an error if the metadata is not valid JSON, or the buffer is incomplete.
pub async fn parse_metadata(
    internal_compression: Compression,
    mut metadata_reader: BufReader<&[u8]>,
) -> Result<Value> {
    let mut decompression_reader = decompress_async(internal_compression, &mut metadata_reader)?;
    let mut json_bytes = Vec::new();
    decompression_reader.read_to_end(&mut json_bytes).await?;
    let val: Value = serde_json::from_slice(&json_bytes[..])?;
    Ok(val)
}

/// Trait implementation for something which just goes to an offset and gets bytes, without any seeking.
#[async_trait::async_trait]
pub trait PMTilesReadSeek {
    async fn get_bytes_at_offset(&self, length: usize, offset: usize) -> Result<Bytes>;
}

pub struct PMTilesReadSeeker<R> {
    reader: Arc<RwLock<R>>,
}

impl<R> PMTilesReadSeeker<R>
where
    R: AsyncSeek + AsyncRead + Sync + Send + Unpin,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: Arc::new(RwLock::new(reader)),
        }
    }
}

#[async_trait::async_trait]
impl<R> PMTilesReadSeek for PMTilesReadSeeker<R>
where
    R: AsyncSeek + AsyncRead + Sync + Send + Unpin,
{
    async fn get_bytes_at_offset(&self, length: usize, offset: usize) -> Result<Bytes> {
        let mut reader = self.reader.write().await;

        reader
            .seek(SeekFrom::Start(offset.try_into().unwrap()))
            .await?;

        let mut buf = vec![0; length];

        reader.read_exact(&mut buf).await?;
        drop(reader);

        Ok(Bytes::from(buf))
    }
}

// #[cfg(test)]
// mod test {
//     use std::io::Cursor;

//     use anyhow::Result;
//     use serde_json::json;
//     use tokio::{fs::File, io::BufReader};

//     use crate::{
//         reader::{parse_metadata, PMTilesReader},
//         Compression, TileType,
//     };

//     const PM_TILES_BYTES: &[u8] =
//         include_bytes!("../test/stamen_toner(raster)CC-BY+ODbL_z3.pmtiles");

//     const PM_TILES_BYTES2: &[u8] = include_bytes!("../test/protomaps(vector)ODbL_firenze.pmtiles");

//     #[tokio::test]
//     async fn test_parse_meta_data() -> Result<()> {
//         let meta_data = parse_metadata(
//             Compression::GZip,
//             BufReader::new(&PM_TILES_BYTES[373..373 + 22]),
//         )
//         .await?;
//         assert_eq!(meta_data, json!({}));

//         let meta_data2 = parse_metadata(
//             Compression::GZip,
//             BufReader::new(&PM_TILES_BYTES2[530..530 + 266]),
//         )
//         .await?;

//         assert_eq!(
//             meta_data2,
//             json!({
//                 "attribution":"<a href=\"https://protomaps.com\" target=\"_blank\">Protomaps</a> © <a href=\"https://www.openstreetmap.org\" target=\"_blank\"> OpenStreetMap</a>",
//                 "tilestats":{
//                     "layers":[
//                         {"geometry":"Polygon","layer":"earth"},
//                         {"geometry":"Polygon","layer":"natural"},
//                         {"geometry":"Polygon","layer":"land"},
//                         {"geometry":"Polygon","layer":"water"},
//                         {"geometry":"LineString","layer":"physical_line"},
//                         {"geometry":"Polygon","layer":"buildings"},
//                         {"geometry":"Point","layer":"physical_point"},
//                         {"geometry":"Point","layer":"places"},
//                         {"geometry":"LineString","layer":"roads"},
//                         {"geometry":"LineString","layer":"transit"},
//                         {"geometry":"Point","layer":"pois"},
//                         {"geometry":"LineString","layer":"boundaries"},
//                         {"geometry":"Polygon","layer":"mask"}
//                     ]
//                 }
//             })
//         );

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_from_reader() -> Result<()> {
//         let cursor = Cursor::new(PM_TILES_BYTES);
//         let reader = BufReader::new(cursor);
//         let pm_tiles = PMTilesReader::new_without_tile_reader(reader).await?;

//         assert_eq!(pm_tiles.header.tile_type, TileType::Png);
//         assert_eq!(pm_tiles.header.internal_compression, Compression::GZip);
//         assert_eq!(pm_tiles.header.tile_compression, Compression::None);
//         assert_eq!(pm_tiles.header.min_zoom, 0);
//         assert_eq!(pm_tiles.header.max_zoom, 3);
//         assert_eq!(pm_tiles.header.center_zoom, 0);
//         assert!((-180.0 - pm_tiles.header.min_pos.longitude).abs() < f64::EPSILON);
//         assert!((-85.0 - pm_tiles.header.min_pos.latitude).abs() < f64::EPSILON);
//         assert!((180.0 - pm_tiles.header.max_pos.longitude).abs() < f64::EPSILON);
//         assert!((85.0 - pm_tiles.header.max_pos.latitude).abs() < f64::EPSILON);
//         assert!(pm_tiles.header.center_pos.longitude < f64::EPSILON);
//         assert!(pm_tiles.header.center_pos.latitude < f64::EPSILON);
//         assert_eq!(pm_tiles.metadata, Some(json!({})));
//         assert_eq!(pm_tiles.num_tiles(), 85);

//         Ok(())
//     }

//     #[tokio::test]
//     async fn test_from_reader2() -> Result<()> {
//         // let cursor = Cursor::new(PM_TILES_BYTES);
//         // let reader = BufReader::new(cursor);
//         // let pm_tiles = PMTilesReader::new(reader).await?;
//         let mut reader = File::open("./test/protomaps(vector)ODbL_firenze.pmtiles").await?;

//         let pm_tiles = PMTilesReader::new_without_tile_reader(&mut reader).await?;

//         assert_eq!(pm_tiles.header.tile_type, TileType::Mvt);
//         assert_eq!(pm_tiles.header.internal_compression, Compression::GZip);
//         assert_eq!(pm_tiles.header.tile_compression, Compression::GZip);
//         assert_eq!(pm_tiles.header.min_zoom, 0);
//         assert_eq!(pm_tiles.header.max_zoom, 14);
//         assert_eq!(pm_tiles.header.center_zoom, 0);
//         assert!((pm_tiles.header.min_pos.longitude - 11.154_026).abs() < f64::EPSILON);
//         assert!((pm_tiles.header.min_pos.latitude - 43.727_012_5).abs() < f64::EPSILON);
//         assert!((pm_tiles.header.max_pos.longitude - 11.328_939_5).abs() < f64::EPSILON);
//         assert!((pm_tiles.header.max_pos.latitude - 43.832_545_5).abs() < f64::EPSILON);
//         assert!((pm_tiles.header.center_pos.longitude - 11.241_482_7).abs() < f64::EPSILON);
//         assert!((pm_tiles.header.center_pos.latitude - 43.779_779).abs() < f64::EPSILON);
//         assert_eq!(
//             pm_tiles.metadata,
//             Some(json!({
//                 "attribution":"<a href=\"https://protomaps.com\" target=\"_blank\">Protomaps</a> © <a href=\"https://www.openstreetmap.org\" target=\"_blank\"> OpenStreetMap</a>",
//                 "tilestats":{
//                     "layers":[
//                         {"geometry":"Polygon","layer":"earth"},
//                         {"geometry":"Polygon","layer":"natural"},
//                         {"geometry":"Polygon","layer":"land"},
//                         {"geometry":"Polygon","layer":"water"},
//                         {"geometry":"LineString","layer":"physical_line"},
//                         {"geometry":"Polygon","layer":"buildings"},
//                         {"geometry":"Point","layer":"physical_point"},
//                         {"geometry":"Point","layer":"places"},
//                         {"geometry":"LineString","layer":"roads"},
//                         {"geometry":"LineString","layer":"transit"},
//                         {"geometry":"Point","layer":"pois"},
//                         {"geometry":"LineString","layer":"boundaries"},
//                         {"geometry":"Polygon","layer":"mask"}
//                     ]
//                 }
//             }))
//         );
//         assert_eq!(pm_tiles.num_tiles(), 108);

//         Ok(())
//     }

//     #[tokio::test]
//     #[allow(clippy::too_many_lines)]
//     async fn test_from_reader3() -> Result<()> {
//         let mut reader =
//             File::open("./test/protomaps_vector_planet_odbl_z10_without_data.pmtiles").await?;

//         let pm_tiles = PMTilesReader::new(&mut reader, None).await?;

//         assert_eq!(pm_tiles.header.tile_type, TileType::Mvt);
//         assert_eq!(pm_tiles.header.internal_compression, Compression::GZip);
//         assert_eq!(pm_tiles.header.tile_compression, Compression::GZip);
//         assert_eq!(pm_tiles.header.min_zoom, 0);
//         assert_eq!(pm_tiles.header.max_zoom, 10);
//         assert_eq!(pm_tiles.header.center_zoom, 0);
//         assert!((-180.0 - pm_tiles.header.min_pos.longitude).abs() < f64::EPSILON);
//         assert!((-90.0 - pm_tiles.header.min_pos.latitude).abs() < f64::EPSILON);
//         assert!((180.0 - pm_tiles.header.max_pos.longitude).abs() < f64::EPSILON);
//         assert!((90.0 - pm_tiles.header.max_pos.latitude).abs() < f64::EPSILON);
//         assert!(pm_tiles.header.center_pos.longitude < f64::EPSILON);
//         assert!(pm_tiles.header.center_pos.latitude < f64::EPSILON);
//         assert_eq!(
//             pm_tiles.metadata,
//             Some(json!({
//                 "attribution": "<a href=\"https://protomaps.com\" target=\"_blank\">Protomaps</a> © <a href=\"https://www.openstreetmap.org\" target=\"_blank\"> OpenStreetMap</a>",
//                 "name": "protomaps 2022-11-08T03:35:13Z",
//                 "tilestats": {
//                     "layers": [
//                         { "geometry": "Polygon", "layer": "earth" },
//                         { "geometry": "Polygon", "layer": "natural" },
//                         { "geometry": "Polygon", "layer": "land" },
//                         { "geometry": "Polygon", "layer": "water" },
//                         { "geometry": "LineString", "layer": "physical_line" },
//                         { "geometry": "Polygon", "layer": "buildings" },
//                         { "geometry": "Point", "layer": "physical_point" },
//                         { "geometry": "Point", "layer": "places" },
//                         { "geometry": "LineString", "layer": "roads" },
//                         { "geometry": "LineString", "layer": "transit" },
//                         { "geometry": "Point", "layer": "pois" },
//                         { "geometry": "LineString", "layer": "boundaries" },
//                         { "geometry": "Polygon", "layer": "mask" }
//                     ]
//                 },
//                 "vector_layers": [
//                     {
//                         "fields": {},
//                         "id": "earth"
//                     },
//                     {
//                         "fields": {
//                             "boundary": "string",
//                             "landuse": "string",
//                             "leisure": "string",
//                             "name": "string",
//                             "natural": "string"
//                         },
//                         "id": "natural"
//                     },
//                     {
//                         "fields": {
//                             "aeroway": "string",
//                             "amenity": "string",
//                             "area:aeroway": "string",
//                             "highway": "string",
//                             "landuse": "string",
//                             "leisure": "string",
//                             "man_made": "string",
//                             "name": "string",
//                             "place": "string",
//                             "pmap:kind": "string",
//                             "railway": "string",
//                             "sport": "string"
//                         },
//                         "id": "land"
//                     },
//                     {
//                         "fields": {
//                             "landuse": "string",
//                             "leisure": "string",
//                             "name": "string",
//                             "natural": "string",
//                             "water": "string",
//                             "waterway": "string"
//                         },
//                         "id": "water"
//                     },
//                     {
//                         "fields": {
//                             "natural": "string",
//                             "waterway": "string"
//                         },
//                         "id": "physical_line"
//                     },
//                     {
//                         "fields": {
//                             "building:part": "string",
//                             "height": "number",
//                             "layer": "string",
//                             "name": "string"
//                         },
//                         "id": "buildings"
//                     },
//                     {
//                         "fields": {
//                             "ele": "number",
//                             "name": "string",
//                             "natural": "string",
//                             "place": "string"
//                         },
//                         "id": "physical_point"
//                     },
//                     {
//                         "fields": {
//                             "capital": "string",
//                             "country_code_iso3166_1_alpha_2": "string",
//                             "name": "string",
//                             "place": "string",
//                             "pmap:kind": "string",
//                             "pmap:rank": "string",
//                             "population": "string"
//                         },
//                         "id": "places"
//                     },
//                     {
//                         "fields": {
//                             "bridge": "string",
//                             "highway": "string",
//                             "layer": "string",
//                             "oneway": "string",
//                             "pmap:kind": "string",
//                             "ref": "string",
//                             "tunnel": "string"
//                         },
//                         "id": "roads"
//                     },
//                     {
//                         "fields": {
//                             "aerialway": "string",
//                             "aeroway": "string",
//                             "highspeed": "string",
//                             "layer": "string",
//                             "name": "string",
//                             "network": "string",
//                             "pmap:kind": "string",
//                             "railway": "string",
//                             "ref": "string",
//                             "route": "string",
//                             "service": "string"
//                         },
//                         "id": "transit"
//                     },
//                     {
//                         "fields": {
//                             "amenity": "string",
//                             "cuisine": "string",
//                             "name": "string",
//                             "railway": "string",
//                             "religion": "string",
//                             "shop": "string",
//                             "tourism": "string"
//                         },
//                         "id": "pois"
//                     },
//                     {
//                         "fields": {
//                             "pmap:min_admin_level": "number"
//                         },
//                         "id": "boundaries"
//                     },
//                     {
//                         "fields": {},
//                         "id": "mask"
//                     }
//                 ]
//             }))
//         );
//         assert_eq!(pm_tiles.num_tiles(), 1_398_101);

//         Ok(())
//     }
// }
