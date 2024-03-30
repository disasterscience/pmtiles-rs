//! # `Tokio-PMTiles`
//!
//! A low level implementation of [the `PMTiles` format](https://github.com/protomaps/PMTiles)
//! refactored for Tokio and heavy write workloads. Originally forked from
//! [pmtiles-rs, pmtiles2 crate](https://github.com/arma-place/pmtiles-rs/). Open to upstreaming, but there's a few
//! discretionary changes largely to improve DX and performance.
//!
//! ## Examples
//!
//! ### Reading from a file
//! ```rust
//! use tokio::fs::File;
//! use pmtiles2::PMTilesReader;
//!
//! #[tokio::main]
//! async fn main () -> anyhow::Result<()> {
//!     let file_path = "./test/stamen_toner(raster)CC-BY+ODbL_z3.pmtiles";
//!
//!     let mut file = File::open(file_path).await?;
//!     let pm_tiles = PMTilesReader::new(file).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//!
//! ### Creating a `PMTiles` archive from scratch
//! ```rust
//! use pmtiles2::{PMTilesWriter, TileType, Compression, util::tile_id};
//! use std::io::Cursor;
//! use std::path::PathBuf;
//! use tokio::fs::File;
//!
//! #[tokio::main]
//! async fn main () -> anyhow::Result<()> {
//!     // create temp directory
//!     let dir = temp_dir::TempDir::new()?;
//!     let file_path = dir.path().join("foo.pmtiles");
//!     let mut pm_tiles = PMTilesWriter::new(TileType::Png, Compression::GZip);
//!
//!     pm_tiles.add_tile(tile_id(0, 0, 0), PathBuf::from("./test/act/14/14969/6467.png"));
//!     pm_tiles.add_tile(tile_id(0, 0, 0), PathBuf::from("./test/act/14/14969/6468.png"));
//!
//!     let mut file = File::create(file_path).await?;
//!     pm_tiles.write(&mut file).await?;
//!
//!     Ok(())
//! }
//! ```

// #![warn(missing_docs)]
#![warn(clippy::cargo)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::multiple_crate_versions)]
#![allow(clippy::cargo_common_metadata)] // Fix tiff package warning
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

mod directory;

// #[allow(clippy::ignored_unit_patterns)]
mod header;

/// Manages tile operations
mod tiles;

/// Utilities for reading and writing `PMTiles` archives.
pub mod util;

/// Errors for the module
pub mod error;

/// Reads a `PMTiles` archive.
pub mod reader;

/// Writes a `PMTiles` archive.
pub mod writer;

pub use directory::{Directory, Entry};
pub use header::{Compression, Header, TileType};
pub use reader::PMTilesReader;
pub use writer::PMTilesWriter;
