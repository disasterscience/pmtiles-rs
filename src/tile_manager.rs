use crate::{error::PmTilesError, util::tile_id, Directory, Entry};
use anyhow::Result;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

#[derive(Debug, Clone)]
pub struct Tile {
    pub backend: TileBackend,
    #[allow(clippy::struct_field_names)]
    pub tile_id: u64,
    pub hash: u64,
    pub len: u32,
}

pub struct FinalisedTiles {
    pub num_addressed_tiles: u64,
    pub num_tile_entries: u64,
    pub num_tile_content: u64,
    pub directory: Directory,
    pub tiles: Vec<Tile>,
    pub total_tile_length: u64,
}

#[derive(Debug, Clone)]
pub enum TileBackend {
    InMemory(Vec<u8>),
    OnDisk(PathBuf),
}

impl Tile {
    pub fn new(tile_id: u64, backend: TileBackend) -> Result<Self> {
        Ok(Self {
            hash: backend.calculate_hash()?,
            len: backend.len()?,
            backend,
            tile_id,
        })
    }

    pub async fn read_payload(&self) -> Result<Vec<u8>> {
        match &self.backend {
            TileBackend::InMemory(payload) => Ok(payload.clone()),
            TileBackend::OnDisk(path) => {
                let data = tokio::fs::read(path).await?;
                Ok(data)
            }
        }
    }
}

impl TileBackend {
    /// Calculate a hash from either the vec of bytes or the file at the path
    pub fn calculate_hash(&self) -> Result<u64> {
        match self {
            Self::InMemory(data) => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(data);
                let finalised_bytes = hasher.finalize();
                let bytes = finalised_bytes.as_bytes();

                // Take the first 8 bytes from the hash and convert them to a u64
                Ok(u64::from_le_bytes(bytes[0..8].try_into()?))
            }

            // The hash is calculated using the BLAKE3 algorithm (multi-threaded, fast, secure hash function
            Self::OnDisk(path) => {
                let mut hasher = blake3::Hasher::new();
                hasher.update_mmap_rayon(path)?;

                let finalised_bytes = hasher.finalize();
                let bytes = finalised_bytes.as_bytes();

                // Take the first 8 bytes from the hash and convert them to a u64
                Ok(u64::from_le_bytes(bytes[0..8].try_into()?))
            }
        }
    }

    pub fn len(&self) -> Result<u32> {
        match self {
            Self::InMemory(data) => Ok(data.len() as u32),
            Self::OnDisk(path) => {
                let metadata = std::fs::metadata(path)?;
                Ok(metadata.len() as u32)
            }
        }
    }
}

impl From<Vec<u8>> for TileBackend {
    fn from(val: Vec<u8>) -> Self {
        Self::InMemory(val)
    }
}

impl From<PathBuf> for TileBackend {
    fn from(val: PathBuf) -> Self {
        Self::OnDisk(val)
    }
}

#[derive(Debug, Default)]
pub struct TileManager {
    /// Lookup by hash, and retrieve tile
    hash_to_tile: HashMap<u64, Tile>,

    /// Lookup by tile ID, retrive tile_id and get hash
    tile_id_to_hash: HashMap<u64, u64>,

    /// Lookup by hash, and resolve to all all tile IDs
    hash_to_tile_ids: HashMap<u64, HashSet<u64>>,
}

impl TileManager {
    /// Adds a tile to this `PMTiles` archive.
    ///
    /// Note that the data should already be compressed if [`Self::tile_compression`] is set to a value other than [`Compression::None`].
    /// The data will **NOT** be compressed automatically.
    /// The [`util`-module](crate::util) includes utilities to compress data.
    ///
    /// # Errors
    /// Can error if the read failed or the tile data was not compressed.
    pub fn add_tile(&mut self, tile_id: u64, tile_data: impl Into<TileBackend>) -> Result<()> {
        // Ensure the tile doesn't already exist
        self.remove_tile(tile_id);

        // Create a tile
        let tile = Tile::new(tile_id, tile_data.into())?;

        // Allow the tile to be found by its ID
        self.tile_id_to_hash.insert(tile_id, tile.hash);

        self.hash_to_tile_ids
            .entry(tile.hash)
            .or_default()
            .insert(tile_id);

        // Allow the tile to be found by its hash
        self.hash_to_tile.insert(tile.hash, tile);

        Ok(())
    }

    /// Removes a tile from this archive.
    pub fn remove_tile(&mut self, tile_id: u64) {
        if let Some(hash) = self.tile_id_to_hash.remove(&tile_id) {
            // find set which includes all ids which have this hash
            let ids_with_hash = self.hash_to_tile_ids.entry(hash).or_default();

            // remove current id from set
            ids_with_hash.remove(&tile_id);

            // delete data for this hash, if there are
            // no other ids that reference this hash
            if ids_with_hash.is_empty() {
                self.hash_to_tile.remove(&hash);
                self.hash_to_tile_ids.remove(&hash);
            }
        }
    }

    /// Get vector of all tile ids in this `PMTiles` archive.
    pub fn get_tile_ids(&self) -> Vec<&u64> {
        self.tile_id_to_hash.keys().collect()
    }

    pub fn num_addressed_tiles(&self) -> usize {
        self.tile_id_to_hash.len()
    }

    fn push_entry(entries: &mut Vec<Entry>, tile_id: u64, offset: u64, length: u32) {
        if let Some(last) = entries.last_mut() {
            if tile_id == last.tile_id + u64::from(last.run_length)
                && last.offset == offset
                && last.length == length
            {
                last.run_length += 1;
                return;
            }
        }

        entries.push(Entry {
            tile_id,
            offset,
            length,
            run_length: 1,
        });
    }

    // async fn get_tile_content(
    //     data_by_hash: &HashMap<u64, TileBackend>,
    //     tile: &TileManagerTile,
    // ) -> Result<Option<Vec<u8>>> {
    //     match tile {
    //         TileManagerTile::Hash(hash) => {
    //             if let Some(tile) = data_by_hash.get(hash) {
    //                 Ok(Some(tile.read().await?))
    //             } else {
    //                 Ok(None)
    //             }
    //         }
    //     }
    // }

    /// Get data of a tile by its id.
    ///
    /// The returned data is the raw data, meaning It is NOT uncompressed automatically,
    /// if it was compressed in the first place.
    /// If you need the uncompressed data, take a look at the [`util`-module](crate::util)
    ///
    /// Will return [`Ok`] with an value of [`None`] if no a tile with the specified tile id was found.
    ///
    /// # Errors
    /// Will return [`Err`] if the tile data was not read into memory yet and there was an error while
    /// attempting to read it.
    ///
    pub fn get_tile_by_id(&self, tile_id: u64) -> Option<&Tile> {
        let hash = self.tile_id_to_hash.get(&tile_id)?;
        let tile = self.hash_to_tile.get(hash)?;
        Some(tile)
    }

    pub fn get_tile_by_hash(&self, hash: u64) -> Option<&Tile> {
        self.hash_to_tile.get(&hash)
    }

    /// Returns the data of the tile with the specified coordinates.
    ///
    /// See [`get_tile_by_id`](Self::get_tile_by_id) for further details on the return type.
    ///
    /// # Errors
    /// See [`get_tile_by_id`](Self::get_tile_by_id) for details on possible errors.
    pub fn get_tile_xyz(&mut self, x: u64, y: u64, z: u8) -> Option<&Tile> {
        self.get_tile_by_id(tile_id(z, x, y))
    }

    pub fn build(self) -> Result<FinalisedTiles> {
        type OffsetLen = (u64, u32);

        let mut tiles_sorted_by_id = self.get_tile_ids();
        tiles_sorted_by_id.sort();

        let mut entries = Vec::<Entry>::new();
        let mut current_offset: u64 = 0;

        let mut num_addressed_tiles: u64 = 0;
        let mut num_tile_content: u64 = 0;

        // hash => offset+length
        let mut offset_length_map = HashMap::<u64, OffsetLen>::default();

        let mut tiles: Vec<Tile> = vec![];

        for tile_id in tiles_sorted_by_id {
            let tile = self
                .get_tile_by_id(*tile_id)
                .ok_or(PmTilesError::MissingTileFollowingLookup)?;

            // trace!("tile_id: {:?}, tile: {:?}", tile_id, tile);

            num_addressed_tiles += 1;

            if let Some((offset, length)) = offset_length_map.get(&tile.hash) {
                Self::push_entry(&mut entries, *tile_id, *offset, *length);
            } else {
                let offset = current_offset;

                #[allow(clippy::cast_possible_truncation)]
                let length = tile.len;

                current_offset += u64::from(length);
                num_tile_content += 1;

                Self::push_entry(&mut entries, *tile_id, offset, length.try_into()?);
                offset_length_map.insert(tile.hash, (offset, length.try_into()?));

                tiles.push(tile.clone());
            }
        }

        let num_tile_entries = entries.len() as u64;

        Ok(FinalisedTiles {
            total_tile_length: current_offset,
            tiles,
            directory: entries.into(),
            num_addressed_tiles,
            num_tile_content,
            num_tile_entries,
        })
    }
}

// #[cfg(test)]
// mod test {
//     use super::*;

//     #[test]
//     fn test_get_tile_none() -> Result<()> {
//         let mut manager = TileManager::default();

//         assert!(manager.get_tile(42)?.is_none());

//         Ok(())
//     }

//     #[test]
//     #[allow(clippy::unwrap_used)]
//     fn test_get_tile_some() -> Result<()> {
//         let mut manager = TileManager::default();

//         let contents = vec![1u8, 3, 3, 7, 4, 2];

//         manager.add_tile(42, contents.clone());

//         let opt = manager.get_tile(42)?;

//         assert!(opt.is_some());
//         assert_eq!(opt.unwrap(), contents);

//         Ok(())
//     }

//     #[test]
//     fn test_add_tile() {
//         let mut manager = TileManager::default();

//         manager.add_tile(1337, vec![1, 3, 3, 7, 4, 2]);
//         assert_eq!(manager.data_by_hash.len(), 1);

//         manager.add_tile(42, vec![4, 2, 1, 3, 3, 7]);

//         assert_eq!(manager.data_by_hash.len(), 2);
//     }

//     #[test]
//     fn test_add_tile_dedup() {
//         let mut manager = TileManager::default();

//         let contents = vec![1u8, 3, 3, 7, 4, 2];

//         manager.add_tile(42, contents.clone());
//         manager.add_tile(1337, contents);

//         assert_eq!(manager.data_by_hash.len(), 1);
//     }

//     #[test]
//     fn test_add_tile_update() {
//         let mut manager = TileManager::default();

//         manager.add_tile(1337, vec![1, 3, 3, 7, 4, 2]);
//         assert_eq!(manager.data_by_hash.len(), 1);
//         assert_eq!(manager.tile_by_id.len(), 1);
//         assert_eq!(manager.ids_by_hash.len(), 1);

//         manager.add_tile(1337, vec![4, 2, 1, 3, 3, 7]);
//         assert_eq!(manager.data_by_hash.len(), 1);
//         assert_eq!(manager.tile_by_id.len(), 1);
//         assert_eq!(manager.ids_by_hash.len(), 1);
//     }

//     #[test]
//     fn test_remove_tile() {
//         let mut manager = TileManager::default();

//         manager.add_tile(42, vec![1u8, 3, 3, 7, 4, 2]);

//         assert_eq!(manager.tile_by_id.len(), 1);
//         assert_eq!(manager.data_by_hash.len(), 1);
//         assert_eq!(manager.ids_by_hash.len(), 1);

//         assert!(manager.remove_tile(42));

//         assert_eq!(manager.tile_by_id.len(), 0);
//         assert_eq!(manager.data_by_hash.len(), 0);
//         assert_eq!(manager.ids_by_hash.len(), 0);
//     }

//     #[test]
//     fn test_remove_tile_non_existent() {
//         let mut manager = TileManager::default();

//         let removed = manager.remove_tile(42);

//         assert!(!removed);
//     }

//     #[test]
//     fn test_remove_tile_dupe() {
//         let mut manager = TileManager::default();

//         let contents = vec![1u8, 3, 3, 7, 4, 2];
//         manager.add_tile(69, contents.clone());
//         manager.add_tile(42, contents.clone());
//         manager.add_tile(1337, contents);

//         assert_eq!(manager.data_by_hash.len(), 1);

//         manager.remove_tile(1337);
//         assert_eq!(manager.data_by_hash.len(), 1);
//         assert_eq!(manager.ids_by_hash.len(), 1);

//         manager.remove_tile(69);
//         assert_eq!(manager.data_by_hash.len(), 1);
//         assert_eq!(manager.ids_by_hash.len(), 1);

//         manager.remove_tile(42);
//         assert_eq!(manager.data_by_hash.len(), 0);
//         assert_eq!(manager.ids_by_hash.len(), 0);
//     }

//     #[test]
//     fn test_finish() -> Result<()> {
//         let mut manager = TileManager::default();

//         let tile_0 = vec![0u8, 3, 3, 7, 4, 2];
//         let tile_42 = vec![42u8, 3, 3, 7, 4, 2];
//         let tile_1337 = vec![1u8, 3, 3, 7, 4, 2];

//         manager.add_tile(0, tile_0.clone());
//         manager.add_tile(42, tile_42.clone());
//         manager.add_tile(1337, tile_1337.clone());

//         let result = manager.finish()?;
//         let data = result.data;
//         let directory = result.directory;

//         assert_eq!(data.len(), tile_0.len() + tile_42.len() + tile_1337.len());
//         assert_eq!(directory.len(), 3);
//         assert_eq!(result.num_tile_entries, 3);
//         assert_eq!(result.num_addressed_tiles, 3);
//         assert_eq!(result.num_tile_content, 3);

//         Ok(())
//     }

//     #[test]
//     fn test_finish_dupes() -> Result<()> {
//         let mut manager = TileManager::default();

//         let content = vec![1u8, 3, 3, 7, 4, 2];

//         manager.add_tile(0, content.clone());
//         manager.add_tile(1, vec![1]);
//         manager.add_tile(1337, content.clone());

//         let result = manager.finish()?;
//         let data = result.data;
//         let directory = result.directory;

//         assert_eq!(data.len(), content.len() + 1);
//         assert_eq!(directory.len(), 3);
//         assert_eq!(result.num_tile_entries, 3);
//         assert_eq!(result.num_addressed_tiles, 3);
//         assert_eq!(result.num_tile_content, 2);
//         assert_eq!(directory[0].offset, directory[2].offset);
//         assert_eq!(directory[0].length, directory[2].length);

//         Ok(())
//     }

//     #[test]
//     fn test_finish_dupes_reader() -> Result<()> {
//         let reader = Cursor::new(vec![1u8, 3, 3, 7, 1, 3, 3, 7]);

//         let mut manager = TileManager::new(Some(reader));

//         manager.add_offset_tile(0, 0, 4);
//         manager.add_offset_tile(5, 0, 4);
//         manager.add_offset_tile(10, 4, 4);
//         manager.add_tile(15, vec![1, 3, 3, 7]);
//         manager.add_tile(20, vec![1, 3, 3, 7]);

//         let result = manager.finish()?;
//         let data = result.data;
//         let directory = result.directory;

//         assert_eq!(data.len(), 4);
//         assert_eq!(directory.len(), 5);
//         assert_eq!(result.num_tile_entries, 5);
//         assert_eq!(result.num_addressed_tiles, 5);
//         assert_eq!(result.num_tile_content, 1);
//         assert_eq!(directory[0].offset, 0);
//         assert_eq!(directory[0].length, 4);
//         assert_eq!(directory[1].offset, 0);
//         assert_eq!(directory[1].length, 4);
//         assert_eq!(directory[2].offset, 0);
//         assert_eq!(directory[2].length, 4);
//         assert_eq!(directory[3].offset, 0);
//         assert_eq!(directory[3].length, 4);
//         assert_eq!(directory[4].offset, 0);
//         assert_eq!(directory[4].length, 4);

//         Ok(())
//     }

//     #[test]
//     fn test_finish_run_length() -> Result<()> {
//         let mut manager = TileManager::default();

//         let content = vec![1u8, 3, 3, 7, 4, 2];

//         manager.add_tile(0, content.clone());
//         manager.add_tile(1, content.clone());
//         manager.add_tile(2, content.clone());
//         manager.add_tile(3, content.clone());
//         manager.add_tile(4, content);

//         let result = manager.finish()?;
//         let directory = result.directory;

//         assert_eq!(directory.len(), 1);
//         assert_eq!(directory[0].run_length, 5);
//         assert_eq!(result.num_tile_entries, 1);
//         assert_eq!(result.num_addressed_tiles, 5);
//         assert_eq!(result.num_tile_content, 1);

//         Ok(())
//     }

//     #[test]
//     fn test_finish_clustered() -> Result<()> {
//         let mut manager = TileManager::default();

//         // add tiles in random order
//         manager.add_tile(42, vec![42]);
//         manager.add_tile(1337, vec![13, 37]);
//         manager.add_tile(69, vec![69]);
//         manager.add_tile(1, vec![1]);

//         let result = manager.finish()?;
//         let directory = result.directory;

//         // make sure entries are in asc order
//         assert_eq!(directory[0].tile_id, 1);
//         assert_eq!(directory[1].tile_id, 42);
//         assert_eq!(directory[2].tile_id, 69);
//         assert_eq!(directory[3].tile_id, 1337);

//         // make sure data offsets are in asc order (clustered)
//         assert!(directory[1].offset > directory[0].offset);
//         assert!(directory[2].offset > directory[1].offset);
//         assert!(directory[3].offset > directory[2].offset);

//         Ok(())
//     }
// }
