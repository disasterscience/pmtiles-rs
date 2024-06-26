use crate::util::{compress_async, decompress_async};
use crate::Compression;
use anyhow::Result;
use integer_encoding::{VarIntAsyncReader, VarIntAsyncWriter};
use std::ops::{Index, IndexMut, Range};
use std::slice::SliceIndex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tracing::trace;

/// A structure representing a directory entry.
///
/// A entry includes information on where to find either a leaf directory or one/multiple tiles.
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Entry {
    /// The first tile id this entry is valid for
    pub tile_id: u64,

    /// Offset (in bytes) of first byte of tile of leaf-directory data
    ///
    /// For tiles this offset is relative to the start of the tile data sections.
    /// For leaf directories this offset is relative to the start of the leaf directory sections.
    pub offset: u64,

    /// Amount of bytes
    pub length: u32,

    /// The run length indicates the amount of tiles this entry is valid for.
    /// A run length of `0` indicates that this is in fact a entry containing information
    /// of a leaf directory.
    pub run_length: u32,
}

impl Entry {
    /// Returns the range of tile ids this entry is valid for.
    pub const fn tile_id_range(&self) -> Range<u64> {
        self.tile_id..self.tile_id + self.run_length as u64
    }

    /// Returns `true` if this entry is for a leaf directory and
    /// `false` if this entry is for tile data.
    pub const fn is_leaf_dir_entry(&self) -> bool {
        self.run_length == 0
    }
}

/// A structure representing a directory.
///
/// A directory holds an arbitrary amount of [`Entry`]. You can use [`len`](Self::len), [`is_empty`](Self::is_empty) and
/// [`iter`](Self::iter) to obtain information about that list of entries.
///
/// Use [`from_reader`](Self::from_reader) and [`to_writer`](Self::to_writer) or their respective asynchronous versions ([`from_async_reader`](Self::from_async_reader) and [`to_async_writer`](Self::to_async_writer)) to read and write the directory from / to bytes.
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Directory {
    /// Tiles contained within a directory
    pub entries: Vec<Entry>,
}

impl Directory {
    /// Returns the number of entries in the directory, also referred to as its 'length'.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the directory contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Directory {
    // #[allow(clippy::needless_range_loop)]
    pub async fn from_async_reader<R>(
        reader: &mut R,
        length: u64,
        compression: Compression,
    ) -> Result<Self>
    where
        R: AsyncRead + AsyncSeekExt + Unpin + Send,
    {
        let mut binding = reader.take(length);
        let mut reader = decompress_async(compression, &mut binding)?;

        let num_entries = reader.read_varint_async::<usize>().await?;

        let mut entries = Vec::<Entry>::with_capacity(num_entries);

        // read tile_id
        let mut last_id = 0u64;
        for _ in 0..num_entries {
            let tmp = reader.read_varint_async::<u64>().await?;

            last_id += tmp;
            entries.push(Entry {
                tile_id: last_id,
                length: 0,
                offset: 0,
                run_length: 0,
            });
        }

        // read run_length
        for i in 0..num_entries {
            entries[i].run_length = reader.read_varint_async::<_>().await?;
        }

        // read length
        for i in 0..num_entries {
            entries[i].length = reader.read_varint_async::<_>().await?;
        }

        // read offset
        for i in 0..num_entries {
            let val = reader.read_varint_async::<u64>().await?;

            entries[i].offset = if i > 0 && val == 0 {
                entries[i - 1].offset + u64::from(entries[i - 1].length)
            } else {
                val - 1
            };
        }

        Ok(Self { entries })
    }

    /// Reads a directory from a [`futures::io::AsyncRead`](https://docs.rs/futures/latest/futures/io/trait.AsyncRead.html) and returns it.
    ///
    /// # Errors
    ///
    pub async fn to_async_writer(
        &self,
        output: &mut (impl AsyncWrite + Unpin + Send),
        compression: &Compression,
    ) -> Result<()> {
        let mut writer = compress_async(compression, output)?;

        writer.write_varint_async(self.entries.len()).await?;

        // write tile_id
        let mut last_id = 0u64;
        for entry in &self.entries {
            trace!("entry.tile_id: {} - last_id: {}", entry.tile_id, last_id);
            writer.write_varint_async(entry.tile_id - last_id).await?;
            last_id = entry.tile_id;
        }

        // write run_length
        for entry in &self.entries {
            writer.write_varint_async(entry.run_length).await?;
        }

        // write length
        for entry in &self.entries {
            writer.write_varint_async(entry.length).await?;
        }

        // write offset
        let mut next_byte = 0u64;
        for (index, entry) in self.entries.iter().enumerate() {
            let val = if index > 0 && entry.offset == next_byte {
                0
            } else {
                entry.offset + 1
            };

            writer.write_varint_async(val).await?;

            next_byte = entry.offset + u64::from(entry.length);
        }

        writer.shutdown().await?;

        Ok(())
    }
}

impl Directory {
    /// Reads a directory from a [`std::io::Read`] and returns it.
    ///
    /// # Arguments
    /// * `input` - Reader including directory bytes
    /// * `length` - Length of the directory (in bytes)
    /// * `compression` - Compression of the directory
    ///
    /// # Errors
    /// Will return [`Err`] if `compression` is set to [`Compression::Unknown`], the data is not compressed correctly
    /// according to `compression` or an I/O error occurred while reading from `input`.
    ///
    /// # Example
    /// ```rust
    /// # use pmtiles2::{Directory, Compression};
    /// # use std::io::{Cursor, Seek, SeekFrom};
    /// let bytes = include_bytes!("../test/stamen_toner(raster)CC-BY+ODbL_z3.pmtiles");
    /// let mut reader = Cursor::new(bytes);
    /// reader.seek(SeekFrom::Start(127)).unwrap();
    ///
    /// let directory = Directory::from_reader(&mut reader, 246, Compression::GZip).unwrap();
    /// ```
    // pub fn from_reader(
    //     input: &mut impl Read,
    //     length: u64,
    //     compression: Compression,
    // ) -> Result<Self> {
    //     Self::from_reader_impl(input, length, compression)
    // }

    /// Reads a directory from anything that can be turned into a byte slice (e.g. [`Vec<u8>`]).
    ///
    /// # Arguments
    /// * `bytes` - Input bytes
    /// * `compression` - Compression of the directory
    ///
    /// # Errors
    /// Will return [`Err`] an I/O error occurred while reading from `input`.
    ///
    /// # Example
    /// ```rust
    /// # use pmtiles2::{Directory, Compression};
    /// let bytes = include_bytes!("../test/stamen_toner(raster)CC-BY+ODbL_z3.pmtiles");
    /// let directory = Directory::from_bytes(&bytes[127..], Compression::GZip).unwrap();
    /// ```
    ///
    // pub fn from_bytes(bytes: impl AsRef<[u8]>, compression: Compression) -> std::io::Result<Self> {
    //     let length = bytes.as_ref().len() as u64;
    //     let mut reader = std::io::Cursor::new(bytes);

    //     Self::from_reader(&mut reader, length, compression)
    // }

    /// Async version of [`from_reader`](Self::from_reader).
    ///
    /// Reads a directory from a [`futures::io::AsyncRead`](https://docs.rs/futures/latest/futures/io/trait.AsyncRead.html) and returns it.
    ///
    /// # Arguments
    /// * `input` - Reader including directory bytes
    /// * `length` - Length of the directory (in bytes)
    /// * `compression` - Compression of the directory
    ///
    /// # Errors
    /// Will return [`Err`] if `compression` is set to [`Compression::Unknown`], the data is not compressed correctly
    /// according to `compression` or an I/O error occurred while reading from `input`.
    ///
    /// # Example
    /// ```rust
    /// # use pmtiles2::{Directory, Compression};
    /// # use futures::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};
    /// # tokio_test::block_on(async {
    /// let bytes = include_bytes!("../test/stamen_toner(raster)CC-BY+ODbL_z3.pmtiles");
    /// let mut reader = futures::io::Cursor::new(bytes);
    /// reader.seek(SeekFrom::Start(127)).await.unwrap();
    ///
    /// let directory = Directory::from_async_reader(&mut reader, 246, Compression::GZip).await.unwrap();
    /// # })
    /// ```
    #[cfg(feature = "async")]
    pub async fn from_async_reader(
        input: &mut (impl AsyncRead + Unpin + Send + AsyncReadExt),
        length: u64,
        compression: Compression,
    ) -> Result<Self> {
        Self::from_async_reader_impl(input, length, compression).await
    }

    /// Writes the directory to a [`std::io::Write`].
    ///
    /// # Arguments
    /// * `output` - Writer to write directory to
    /// * `compression` - Compression to use
    ///
    /// # Errors
    /// Will return [`Err`] if `compression` is set to [`Compression::Unknown`] or an I/O
    /// error occurred while writing to `output`.
    ///
    /// # Example
    /// ```rust
    /// # use pmtiles2::{Directory, Compression};
    /// let directory: Directory = Vec::new().into();
    ///
    /// let mut output = std::io::Cursor::new(Vec::<u8>::new());
    ///
    /// directory.to_writer(&mut output, Compression::GZip).unwrap();
    /// ```
    // pub fn to_writer(&self, output: &mut impl Write, compression: Compression) -> Result<()> {
    //     self.to_writer_impl(output, compression)
    // }

    /// Async version of [`to_writer`](Self::to_writer).
    ///
    /// Writes the directory to a [`futures::io::AsyncWrite`](https://docs.rs/futures/latest/futures/io/trait.AsyncWrite.html).
    ///
    /// # Arguments
    /// * `output` - Writer to write directory to
    /// * `compression` - Compression to use
    ///
    /// # Errors
    /// Will return [`Err`] if `compression` is set to [`Compression::Unknown`] or an I/O
    /// error occurred while writing to `output`.
    ///
    /// # Example
    /// ```rust
    /// # use pmtiles2::{Directory, Compression};
    /// # tokio_test::block_on(async {
    /// let directory: Directory = Vec::new().into();
    ///
    /// let mut output = futures::io::Cursor::new(Vec::<u8>::new());
    ///
    /// directory.to_async_writer(&mut output, Compression::GZip).await.unwrap();
    /// # })
    /// ```
    #[cfg(feature = "async")]
    pub async fn to_async_writer(
        &self,
        output: &mut (impl AsyncWrite + Unpin + Send),
        compression: Compression,
    ) -> Result<()> {
        self.to_async_writer_impl(output, compression).await
    }
}

impl Directory {
    /// Find a entry, which includes given `tile_id`.
    ///
    /// Returns [`None`] if the directory does not include a [`Entry`] that matches `tile_id`.
    ///
    pub fn find_entry_for_tile_id(&self, tile_id: u64) -> Option<&Entry> {
        self.entries
            .iter()
            .find(|e| !e.is_leaf_dir_entry() && e.tile_id_range().contains(&tile_id))
    }
}

impl<I: SliceIndex<[Entry]>> Index<I> for Directory {
    type Output = I::Output;

    fn index(&self, index: I) -> &Self::Output {
        self.entries.index(index)
    }
}

impl<I: SliceIndex<[Entry]>> IndexMut<I> for Directory {
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        self.entries.index_mut(index)
    }
}

impl From<Vec<Entry>> for Directory {
    fn from(entries: Vec<Entry>) -> Self {
        Self { entries }
    }
}

impl From<Directory> for Vec<Entry> {
    fn from(val: Directory) -> Self {
        val.entries
    }
}

// #[cfg(test)]
// #[allow(clippy::cast_possible_truncation)]
// mod test {
//     use std::io::{Cursor, Seek, SeekFrom};

//     use crate::util::decompress_all;

//     use super::*;

//     const PM_TILES_BYTES: &[u8] =
//         include_bytes!("../test/stamen_toner(raster)CC-BY+ODbL_z3.pmtiles");

//     const ROOT_DIR_OFFSET: u64 = 127;
//     const ROOT_DIR_LENGTH: u64 = 246;
//     const ROOT_DIR_COMPRESSION: Compression = Compression::GZip;

//     #[test]
//     fn test_from_reader() -> Result<()> {
//         let mut reader = Cursor::new(PM_TILES_BYTES);
//         reader.seek(SeekFrom::Start(ROOT_DIR_OFFSET))?;

//         let dir = Directory::from_reader(&mut reader, ROOT_DIR_LENGTH, ROOT_DIR_COMPRESSION)?;

//         assert_eq!(reader.position(), ROOT_DIR_OFFSET + ROOT_DIR_LENGTH);
//         assert_eq!(dir.entries.len(), 84);
//         assert_eq!(
//             dir.entries[0],
//             Entry {
//                 tile_id: 0,
//                 offset: 0,
//                 length: 18404,
//                 run_length: 1
//             }
//         );

//         assert_eq!(
//             dir.entries[58],
//             Entry {
//                 tile_id: 58,
//                 offset: 422_070,
//                 length: 850,
//                 run_length: 2
//             }
//         );

//         assert_eq!(
//             dir.entries[83],
//             Entry {
//                 tile_id: 84,
//                 offset: 243_790,
//                 length: 914,
//                 run_length: 1
//             }
//         );

//         Ok(())
//     }

//     #[test]
//     fn test_to_writer() -> Result<()> {
//         let mut reader = Cursor::new(PM_TILES_BYTES);
//         reader.seek(SeekFrom::Start(ROOT_DIR_OFFSET))?;

//         let dir = Directory::from_reader(&mut reader, ROOT_DIR_LENGTH, ROOT_DIR_COMPRESSION)?;

//         let mut buf = Vec::<u8>::with_capacity(ROOT_DIR_LENGTH as usize);
//         let mut writer = Cursor::new(&mut buf);
//         dir.to_writer(&mut writer, ROOT_DIR_COMPRESSION)?;

//         // we compare the decompressed versions of the directory, as the compressed
//         // bytes may not match 100% due to different compression parameters
//         let output = decompress_all(ROOT_DIR_COMPRESSION, &buf)?;
//         let expected = decompress_all(
//             ROOT_DIR_COMPRESSION,
//             &PM_TILES_BYTES[ROOT_DIR_OFFSET as usize..(ROOT_DIR_OFFSET + ROOT_DIR_LENGTH) as usize],
//         )?;

//         assert_eq!(output, expected);

//         Ok(())
//     }
// }
