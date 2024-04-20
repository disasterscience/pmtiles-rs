use crate::{
    error::PmTilesError,
    util::{tile_id, OffsetLength},
    Directory, Entry,
};
use anyhow::Result;
use futures::{stream::FuturesUnordered, StreamExt};
use std::{
    collections::{HashMap, HashSet},
    io::{SeekFrom, Write},
    path::{Path, PathBuf},
};
use tracing::{trace, warn};
use walkdir::WalkDir;

use serde_json::{json, Value};
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::mpsc,
    task::JoinHandle,
};

use crate::{
    header::HEADER_BYTES,
    tiles::{Tile, TileBackend},
    util::{compress_async, write_directories_async, WriteDirsOverflowStrategy},
    Compression, Header, TileType,
};
use std::os::unix::fs::FileExt;

/// Manages writing a `PMTiles` archive
#[allow(clippy::module_name_repetitions)]
pub struct PMTilesWriter {
    pub header: Header,

    /// JSON meta data of this archive
    pub metadata: Option<Value>,

    /// Lookup by hash, and retrieve tile
    pub hash_to_tile: HashMap<u64, Tile>,

    /// Lookup by tile ID, retrive tile_id and get hash
    pub tile_id_to_hash: HashMap<u64, u64>,

    /// Lookup by hash, and resolve to all all tile IDs
    pub hash_to_tile_ids: HashMap<u64, HashSet<u64>>,
}

impl PMTilesWriter {
    /// Constructs a new, empty `PMTiles` archive, with no metadata, an [`internal_compression`](Self::internal_compression) of GZIP and all numeric fields set to `0`.
    ///
    /// # Arguments
    /// * `tile_type` - Type of tiles in this archive
    /// * `tile_compression` - Compression of tiles in this archive
    pub fn new(tile_type: TileType, tile_compression: Compression) -> Self {
        Self {
            header: Header {
                tile_compression,
                tile_type,
                ..Default::default()
            },
            metadata: None,
            hash_to_tile: HashMap::new(),
            tile_id_to_hash: HashMap::new(),
            hash_to_tile_ids: HashMap::new(),
        }
    }

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
        // self.remove_tile(tile_id);

        // Create a tile
        let tile = Tile::new(tile_id, tile_data.into())?;

        self.add(tile);

        Ok(())
    }

    /// Adds a tile to this `PMTiles` archive, from xyz coordinates.
    ///
    /// Note that the data should already be compressed if [`Self::tile_compression`] is set to a value other than [`Compression::None`].
    /// The data will **NOT** be compressed automatically.
    /// The [`util`-module](crate::util) includes utilities to compress data.
    ///
    /// # Errors
    /// Can error if the read failed or the tile data was not compressed.
    pub fn add_tile_xyz(
        &mut self,
        z: u8,
        x: u64,
        y: u64,
        tile_data: impl Into<TileBackend>,
    ) -> Result<()> {
        let tile_id = tile_id(z, x, y);
        self.add_tile(tile_id, tile_data)
    }

    pub fn add(&mut self, tile: Tile) {
        // Check if the tile exists already
        // if self.tile_id_to_hash.contains_key(&tile.tile_id) {
        //     return;
        // }

        // Allow the tile to be found by its ID
        self.tile_id_to_hash.insert(tile.tile_id, tile.hash);

        self.hash_to_tile_ids
            .entry(tile.hash)
            .or_default()
            .insert(tile.tile_id);

        // Allow the tile to be found by its hash
        self.hash_to_tile.insert(tile.hash, tile);
    }

    /// Load files from the folder and add to the `PMTiles` archive.
    /// Assumes the folder is structured: /tileset/{z}/{x}/{y}.png
    ///
    /// # Returns
    /// The number of tiles added to the archive.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be read or the files cannot be loaded.
    ///
    /// # Panics
    /// Will panic if an expected internal fails, like queueing a tile to be sent.
    #[allow(clippy::cognitive_complexity)]
    pub async fn add_tiles_from_folder(
        mut self,
        directory: &Path,
    ) -> Result<FinalisedPMTilesWriter> {
        let entries = WalkDir::new(directory)
            .into_iter()
            .filter_map(std::result::Result::ok)
            .collect::<Vec<_>>();

        let tile_type = self.header.tile_type;
        let (tx, mut rx) = mpsc::channel::<Tile>(256);

        // Begin reading tiles, spawn first async to move tx inside (so it's later dropped)
        tokio::spawn(async move {
            for (count, entry) in entries.into_iter().enumerate() {
                let txc = tx.clone();

                // Enumerate directory, creating new tasks for each file
                tokio::spawn(async move {
                    let path = entry.path().to_path_buf();

                    // Spawn a new task for each file
                    if let Some((z, x, y)) = Self::extract_zxy_from_path(&path, tile_type) {
                        // Convert TMS to XYZ
                        let y = (1 << z) - 1 - y;
                        let tile_id = tile_id(z, x, y);

                        // We're assuming small files could be similar, but large files are likely unique so don't bother hashing
                        if let Ok(tile) = Tile::new_with_defined_hash(
                            tile_id,
                            count.try_into().expect("hash failed"),
                            path.into(),
                        ) {
                            txc.send(tile).await.expect("send tile failed");
                        }
                    }
                });
            }
        });

        // Consume the tile data and write it to the output
        while let Some(tile) = rx.recv().await {
            self.add(tile);
        }

        let finalised = self.build()?;

        Ok(finalised)
    }

    /// Given a file path, extract the z, x, and y values from the path
    /// Assuming the structure is /tmp/pmtiles/act/{z}/{x}/{y}.png
    fn extract_zxy_from_path(path: &Path, tile_type: TileType) -> Option<(u8, u64, u64)> {
        let parts: Vec<&str> = path.to_str()?.split('/').collect();

        // Adjust the indices if your base path changes
        if parts.len() >= 3 {
            let z_index = parts.len() - 3;
            let x_index = parts.len() - 2;
            let y_index = parts.len() - 1;

            if let (Ok(z), Ok(x), Some(y_with_extension)) = (
                parts[z_index].parse::<u8>(),
                parts[x_index].parse::<u64>(),
                parts.get(y_index),
            ) {
                // Strip the ".png" extension from y
                let y = y_with_extension
                    .trim_end_matches(tile_type.file_suffix())
                    .parse::<u32>()
                    .ok()?;
                return Some((z, x, y.into()));
            }
        }
        None
    }

    /// Removes a tile from this archive.
    pub fn remove_tile(&mut self, tile_id: u64) -> bool {
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
            true
        } else {
            false
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

    /// Builds the finalised `PMTiles` archive, ready to be written to a writer.
    ///
    /// # Errors
    /// Will return an error if something isn't quite right
    pub fn build(&self) -> Result<FinalisedPMTilesWriter> {
        let mut tiles_sorted_by_id = self.get_tile_ids();
        tiles_sorted_by_id.sort();

        let mut entries = Vec::<Entry>::new();
        let mut current_offset: u64 = 0;

        let mut num_addressed_tiles: u64 = 0;
        let mut num_tile_content: u64 = 0;

        // hash => offset+length
        let mut offset_length_map = HashMap::<u64, OffsetLength>::default();

        let mut tiles: Vec<Tile> = vec![];

        for tile_id in tiles_sorted_by_id {
            let tile = self
                .get_tile_by_id(*tile_id)
                .ok_or(PmTilesError::MissingTileFollowingLookup)?;

            trace!("tile_id: {:?}, tile: {:?}", tile_id, tile);

            num_addressed_tiles += 1;

            if let Some(offset_length) = offset_length_map.get(&tile.hash) {
                Self::push_entry(
                    &mut entries,
                    *tile_id,
                    offset_length.offset,
                    offset_length.length,
                );
            } else {
                let final_tile_offset_length = OffsetLength::new(current_offset, tile.len);

                Self::push_entry(
                    &mut entries,
                    *tile_id,
                    final_tile_offset_length.offset,
                    final_tile_offset_length.length,
                );
                offset_length_map.insert(tile.hash, final_tile_offset_length);

                tiles.push(tile.clone());

                current_offset += u64::from(final_tile_offset_length.length);
                num_tile_content += 1;
            }
        }

        let mut header = self.header.clone();
        header.tile_data_length = current_offset;
        header.num_addressed_tiles = num_addressed_tiles;
        header.num_tile_entries = entries.len() as u64;
        header.num_tile_content = num_tile_content;

        header.root_directory_offset = u64::from(HEADER_BYTES);

        let finalised_tiles = FinalisedPMTilesWriter {
            tiles,
            directory: entries.into(),
            header,
            metadata: self.metadata.clone(),
        };

        Ok(finalised_tiles)
    }
}

impl Default for PMTilesWriter {
    fn default() -> Self {
        Self::new(TileType::Png, Compression::None)
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct FinalisedPMTilesWriter {
    pub header: Header,
    pub metadata: Option<Value>,
    pub directory: Directory,
    pub tiles: Vec<Tile>,
}

impl FinalisedPMTilesWriter {
    /// Write the `PMTiles` archive to a file.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created or the `PMTiles` archive cannot be written.
    pub async fn write_to_file(self, path: PathBuf) -> Result<()> {
        trace!("Writing PMTiles to file: {:?}", &path);

        self.write(&path).await?;

        Ok(())
    }

    /// Writes the archive to a writer.
    ///
    /// # Errors
    /// Will return an error if the internal compression of the archive is set to "Unknown" or an I/O error occurred while writing to `output`.
    #[allow(clippy::wrong_self_convention, clippy::too_many_lines)]
    pub async fn write(
        mut self,
        // output: &mut (impl AsyncWrite + Send + Unpin + AsyncSeekExt),
        path: &Path,
    ) -> Result<()> {
        // Delete the file if it already exists
        if path.exists() {
            std::fs::remove_file(path)?;
        }

        trace!("Writing PMTiles archive");
        let mut output = File::create(path).await?;

        // ROOT DIR
        output
            .seek(SeekFrom::Current(i64::from(HEADER_BYTES)))
            .await?;
        let root_directory_offset = u64::from(HEADER_BYTES);
        let leaf_directories_data = write_directories_async(
            &mut output,
            &self.directory[0..],
            &self.header.internal_compression,
            WriteDirsOverflowStrategy::default(),
        )
        .await?;
        let root_directory_length = output.stream_position().await? - root_directory_offset;

        trace!(
            "Root directory offset: {:x}, len: {}",
            root_directory_offset,
            root_directory_length
        );

        // METADATA
        let json_metadata_offset = root_directory_offset + root_directory_length;
        {
            let metadata = &self.metadata.clone().unwrap_or(json!({}));
            let vec = serde_json::to_vec(metadata)?;
            trace!("Writing metadata: {:?}, {:x?}", metadata, vec);

            let mut compression_writer =
                compress_async(&self.header.internal_compression, &mut output)?;
            compression_writer.write_all(&vec).await?;
            compression_writer.shutdown().await?;
        }
        let json_metadata_length = output.stream_position().await? - json_metadata_offset;

        trace!(
            "Metadata offset: {:x}, len: {}",
            json_metadata_offset,
            json_metadata_length
        );

        // LEAF DIRECTORIES
        let leaf_directories_offset = json_metadata_offset + json_metadata_length;
        output.write_all(&leaf_directories_data[0..]).await?;
        drop(leaf_directories_data);
        let leaf_directories_length = output.stream_position().await? - leaf_directories_offset;

        trace!(
            "Leaf directorry offset: {:x}, len: {}",
            leaf_directories_offset,
            leaf_directories_length
        );

        // Calculate where the data will go
        let tile_data_offset = leaf_directories_offset + leaf_directories_length;
        let tile_count = self.tiles.len();

        // Update header with values we now know
        self.header.root_directory_length = root_directory_length;
        self.header.json_metadata_offset = json_metadata_offset;
        self.header.json_metadata_length = json_metadata_length;
        self.header.leaf_directories_offset = leaf_directories_offset;
        self.header.leaf_directories_length = leaf_directories_length;
        self.header.tile_data_offset = tile_data_offset;

        trace!("Writing header: {:?}", self.header);

        output
            .seek(SeekFrom::Start(
                root_directory_offset - u64::from(HEADER_BYTES),
            ))
            .await?; // jump to start of stream

        self.header.to_async_writer(&mut output).await?;

        output
            .seek(SeekFrom::Start(
                (root_directory_offset - u64::from(HEADER_BYTES))
                    + tile_data_offset
                    + self.header.tile_data_length,
            ))
            .await?; // jump to end of stream

        trace!(
            "Writing tile data offset: {:x}, len: {}, tile count: {}",
            tile_data_offset,
            self.header.tile_data_length,
            tile_count
        );
        output.flush().await?;

        Self::vector_tile_write(self.tiles, tile_data_offset, output).await?;

        Ok(())
    }

    async fn vector_tile_write(
        tiles: Vec<Tile>,
        data_offset: u64,
        // output: &mut (impl AsyncWrite + Send + Unpin + AsyncSeekExt),
        file: File,
    ) -> Result<()> {
        let file = file.try_into_std().expect("cannot convert file");

        // Pre-allocate space for all tiles, early error rather than half way through processing
        // let data_size: u64 = tiles.iter().map(|t| u64::from(t.len)).sum();
        // let total_size: u64 = data_offset + data_size;
        // trace!("Pre-allocating entire output: {}", total_size);
        // output.seek(SeekFrom::Start(data_offset))?;
        // output.write_all(&vec![0; usize::try_from(data_size)?])?;

        // Calculate the offsets for each tile
        let tile_offsets: Vec<u64> = tiles
            .iter()
            .scan(data_offset, |acc, tile| {
                let current_offset = *acc;
                *acc += u64::from(tile.len);
                Some(current_offset)
            })
            .collect();

        // Merge the offsets with the tiles
        let tile_with_offset = tiles
            .into_iter()
            .zip(tile_offsets)
            .collect::<Vec<(Tile, u64)>>();

        // Sort by offset, so we can roughly write in order
        // tile_with_offset.sort_by_key(|(_, offset)| *offset);

        // // Read the tile data and send it to the writer, so ideally reads > writes
        // tokio::spawn(async move {
        //     // Calculate where the tiles will be written

        //     for (tile, offset) in tile_with_offset {
        //         if let Ok(tile_payload) = tile.read_payload().await {
        //             if let Err(e) = tx.send((tile_payload, offset)).await {
        //                 warn!("Failed to send tile data: {:?}", e);
        //             }
        //         }
        //     }
        // });

        // // Consume the tile data and write it to the output
        // let mut count = 0;
        // while let Some((payload, offset)) = rx.recv().await {
        //     if count % 1000 == 0 {
        //         debug!("Writing tile data: {}, offset: {}", count, offset,);
        //     }
        //     output.seek(SeekFrom::Start(offset)).await?;
        //     output.write_all(&payload).await?;
        //     output.flush().await?;
        //     count += 1;
        // }

        // trace!("Wrote {} tiles", count);

        // Ok(())

        // Multi writer implementation.
        // Ensure the writers have enough work to do by fanning out the work
        let mut tasks: FuturesUnordered<JoinHandle<Result<()>>> = FuturesUnordered::new();
        let (tx, rx) = crossbeam_channel::bounded::<(Tile, u64)>(256);

        // Enqueue the tiles
        tasks.push(tokio::spawn(async move {
            for (tile, offset) in tile_with_offset {
                tx.send((tile, offset))?;
            }

            Ok(())
        }));

        // Annoyingly, the underlying file handle seems to be shared
        // let output = std::sync::Arc::new(Mutex::new(file));

        // Create some writer workers
        for worker in 0..16 {
            let rx = rx.clone();

            // Open our own handle
            let mut output = file.try_clone().expect("failed to clone file handle");

            // Spawn the worker
            tasks.push(tokio::spawn(async move {
                let mut task_count = 0;
                while let Ok((tile, offset)) = rx.recv() {
                    if task_count % 1000 == 0 {
                        trace!(
                            "Worker: {}, writing tile data: {}, queue: {}",
                            worker,
                            task_count,
                            rx.len()
                        );
                    }
                    let payload = tile.read_payload().await?;

                    {
                        // Using seek + write requires a lock, as the underlying file descriptor is shared
                        // let mut output = output.lock().await;
                        // output_lock.seek(SeekFrom::Start(offset))?;

                        // Write at doesn't need a lock
                        output.write_at(&payload, offset)?;
                        output.flush()?;
                    }
                    task_count += 1;
                }
                Ok(())
            }));
        }

        // Wait for tasks
        while let Some(finished_task) = tasks.next().await {
            match finished_task {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    warn!("Failed to write tile data: {:?}", e);
                    return Err(e);
                }
                Err(e) => {
                    warn!("Join error writing tile data: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use anyhow::Result;
    use tokio::fs::File;
    use tracing::debug;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    use crate::{Compression, PMTilesReader, PMTilesWriter, TileType};

    fn init_logging() {
        let _ = tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "debug".into()),
            )
            .with(tracing_subscriber::fmt::layer())
            .try_init();
    }

    #[tokio::test]
    async fn test_get_tile_none() -> Result<()> {
        let writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        assert!(writer.get_tile_by_id(42).is_none());

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::unwrap_used)]
    async fn test_get_tile_some() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        let contents = vec![1u8, 3, 3, 7, 4, 2];

        writer.add_tile(42, contents.clone())?;
        let tile = writer.get_tile_by_id(42).unwrap();
        let tile_payload = tile.read_payload().await?;

        assert_eq!(tile_payload, contents);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_tile() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        writer.add_tile(1337, vec![1, 3, 3, 7, 4, 2])?;
        assert_eq!(writer.hash_to_tile.len(), 1);

        writer.add_tile(42, vec![4, 2, 1, 3, 3, 7])?;
        assert_eq!(writer.hash_to_tile.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_tile_dedup() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        let contents = vec![1u8, 3, 3, 7, 4, 2];

        writer.add_tile(42, contents.clone())?;
        writer.add_tile(1337, contents)?;

        assert_eq!(writer.hash_to_tile.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_add_tile_update() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        writer.add_tile(1337, vec![1, 3, 3, 7, 4, 2])?;
        assert_eq!(writer.hash_to_tile.len(), 1);
        assert_eq!(writer.tile_id_to_hash.len(), 1);
        assert_eq!(writer.hash_to_tile_ids.len(), 1);

        writer.add_tile(1337, vec![4, 2, 1, 3, 3, 7])?;
        assert_eq!(writer.hash_to_tile.len(), 1);
        assert_eq!(writer.tile_id_to_hash.len(), 1);
        assert_eq!(writer.hash_to_tile_ids.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_tile() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        writer.add_tile(42, vec![1u8, 3, 3, 7, 4, 2])?;

        assert_eq!(writer.tile_id_to_hash.len(), 1);
        assert_eq!(writer.hash_to_tile.len(), 1);
        assert_eq!(writer.hash_to_tile_ids.len(), 1);

        assert!(writer.remove_tile(42));

        assert_eq!(writer.tile_id_to_hash.len(), 0);
        assert_eq!(writer.hash_to_tile.len(), 0);
        assert_eq!(writer.hash_to_tile_ids.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_remove_tile_non_existent() {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        let removed = writer.remove_tile(42);

        assert!(!removed);
    }

    #[tokio::test]
    async fn test_remove_tile_dupe() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        let contents = vec![1u8, 3, 3, 7, 4, 2];
        writer.add_tile(69, contents.clone())?;
        writer.add_tile(42, contents.clone())?;
        writer.add_tile(1337, contents)?;

        assert_eq!(writer.hash_to_tile.len(), 1);

        writer.remove_tile(1337);
        assert_eq!(writer.hash_to_tile.len(), 1);
        assert_eq!(writer.hash_to_tile_ids.len(), 1);

        writer.remove_tile(69);
        assert_eq!(writer.hash_to_tile.len(), 1);
        assert_eq!(writer.hash_to_tile_ids.len(), 1);

        writer.remove_tile(42);
        assert_eq!(writer.hash_to_tile.len(), 0);
        assert_eq!(writer.hash_to_tile_ids.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_finish() -> Result<()> {
        init_logging();

        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        let tile_0 = vec![0u8, 3, 3, 7, 4, 2];
        let tile_42 = vec![42u8, 3, 3, 7, 4, 2];
        let tile_1337 = vec![1u8, 3, 3, 7, 4, 2];

        writer.add_tile(0, tile_0.clone())?;
        writer.add_tile(42, tile_42.clone())?;
        writer.add_tile(1337, tile_1337.clone())?;

        assert_eq!(writer.hash_to_tile.len(), 3);
        assert_eq!(writer.hash_to_tile_ids.len(), 3);
        assert_eq!(writer.tile_id_to_hash.len(), 3);

        let result = writer.build()?;
        let tile_data_length = usize::try_from(result.header.tile_data_length)?;

        debug!("result: {:?}", result);

        assert_eq!(result.directory.len(), 3);
        assert_eq!(result.header.num_tile_entries, 3);
        assert_eq!(result.header.num_addressed_tiles, 3);
        assert_eq!(result.header.num_tile_content, 3);
        assert_eq!(
            tile_data_length,
            tile_0.len() + tile_42.len() + tile_1337.len()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_finish_dupes() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        let content = vec![1u8, 3, 3, 7, 4, 2];

        writer.add_tile(0, content.clone())?;
        writer.add_tile(1, vec![1])?;
        writer.add_tile(1337, content.clone())?;

        let result = writer.build()?;
        let tile_data_length = usize::try_from(result.header.tile_data_length)?;

        assert_eq!(tile_data_length, content.len() + 1);
        assert_eq!(result.directory.len(), 3);
        assert_eq!(result.header.num_tile_entries, 3);
        assert_eq!(result.header.num_addressed_tiles, 3);
        assert_eq!(result.header.num_tile_content, 2);
        assert_eq!(result.directory[0].offset, result.directory[2].offset);
        assert_eq!(result.directory[0].length, result.directory[2].length);

        Ok(())
    }

    #[tokio::test]
    async fn test_finish_dupes_offsets() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        writer.add_tile(15, vec![1, 3, 3, 7])?;
        writer.add_tile(20, vec![1, 3, 3, 7])?;

        // writer.add_offset_tile(0, 0, 4);
        // writer.add_offset_tile(5, 0, 4);
        // writer.add_offset_tile(10, 4, 4);

        let result = writer.build()?;
        let tile_data_length = usize::try_from(result.header.tile_data_length)?;

        assert_eq!(tile_data_length, 4);
        assert_eq!(result.directory.len(), 2);
        assert_eq!(result.header.num_tile_entries, 2);
        assert_eq!(result.header.num_addressed_tiles, 2);
        assert_eq!(result.header.num_tile_content, 1);
        assert_eq!(result.directory[0].offset, 0);
        assert_eq!(result.directory[0].length, 4);
        assert_eq!(result.directory[1].offset, 0);
        assert_eq!(result.directory[1].length, 4);
        // assert_eq!(result.directory[2].offset, 0);
        // assert_eq!(result.directory[2].length, 4);
        // assert_eq!(result.directory[3].offset, 0);
        // assert_eq!(result.directory[3].length, 4);
        // assert_eq!(result.directory[4].offset, 0);
        // assert_eq!(result.directory[4].length, 4);

        Ok(())
    }

    #[tokio::test]
    async fn test_finish_run_length() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        let content = vec![1u8, 3, 3, 7, 4, 2];

        writer.add_tile(0, content.clone())?;
        writer.add_tile(1, content.clone())?;
        writer.add_tile(2, content.clone())?;
        writer.add_tile(3, content.clone())?;
        writer.add_tile(4, content)?;

        let result = writer.build()?;
        let directory = result.directory;

        assert_eq!(directory.len(), 1);
        assert_eq!(directory[0].run_length, 5);
        assert_eq!(result.header.num_tile_entries, 1);
        assert_eq!(result.header.num_addressed_tiles, 5);
        assert_eq!(result.header.num_tile_content, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_finish_clustered() -> Result<()> {
        let mut writer = PMTilesWriter::new(TileType::Png, Compression::GZip);

        // add tiles in random order
        writer.add_tile(42, vec![42])?;
        writer.add_tile(1337, vec![13, 37])?;
        writer.add_tile(69, vec![69])?;
        writer.add_tile(1, vec![1])?;

        let result = writer.build()?;
        let directory = result.directory;

        // make sure entries are in asc order
        assert_eq!(directory[0].tile_id, 1);
        assert_eq!(directory[1].tile_id, 42);
        assert_eq!(directory[2].tile_id, 69);
        assert_eq!(directory[3].tile_id, 1337);

        // make sure data offsets are in asc order (clustered)
        assert!(directory[1].offset > directory[0].offset);
        assert!(directory[2].offset > directory[1].offset);
        assert!(directory[3].offset > directory[2].offset);

        Ok(())
    }

    // #[tokio::test]
    // async fn test_add_tiles_from_folder() -> Result<()> {
    //     init_logging();

    //     let writer = PMTilesWriter::new(TileType::Png, Compression::None);

    //     let input_folder = std::path::Path::new("test/act");
    //     let output_file = std::path::Path::new("test/test_add_tiles_from_folder.pmtiles");

    //     let result = writer.add_tiles_from_folder(input_folder).await?;

    //     assert_eq!(result.directory.len(), 6);
    //     assert_eq!(result.header.num_tile_entries, 6);
    //     assert_eq!(result.header.num_addressed_tiles, 6);
    //     assert_eq!(result.header.num_tile_content, 6);

    //     result.write_to_file(output_file.to_path_buf()).await?;

    //     let reader = File::open(output_file).await?;
    //     let pmtiles = PMTilesReader::new(reader).await?;

    //     assert_eq!(pmtiles.num_tiles(), 6);
    //     assert_eq!(pmtiles.header.num_tile_entries, 6);
    //     assert_eq!(pmtiles.header.num_addressed_tiles, 6);
    //     assert_eq!(pmtiles.header.num_tile_content, 6);

    //     debug!("read header: {:?}", pmtiles.header);

    //     Ok(())
    // }
}
