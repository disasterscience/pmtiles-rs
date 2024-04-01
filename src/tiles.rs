use anyhow::Result;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct Tile {
    pub backend: TileBackend,
    #[allow(clippy::struct_field_names)]
    pub tile_id: u64,
    pub hash: u64,
    pub len: u32,
}

#[derive(Clone, Debug)]
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

    pub fn new_with_defined_hash(tile_id: u64, hash: u64, backend: TileBackend) -> Result<Self> {
        Ok(Self {
            hash,
            len: backend.len()?,
            backend,
            tile_id,
        })
    }

    pub fn new_only_hash_small(tile_id: u64, hash: u64, backend: TileBackend) -> Result<Self> {
        let len = backend.len()?;

        let hash = if len.lt(&100_000) {
            backend.calculate_hash()?
        } else {
            hash
        };

        Ok(Self {
            backend,
            tile_id,
            hash,
            len,
        })
    }

    pub async fn read_payload(&self) -> Result<Vec<u8>> {
        match &self.backend {
            TileBackend::InMemory(payload) => Ok(payload.clone()),
            TileBackend::OnDisk(path) => {
                let data = tokio::fs::read(path).await?;
                Ok(data)
            } // TileBackend::Remote(reader, offset_len) => {
              //     let mut reader = reader.write().await;
              //     reader.seek(SeekFrom::Start(offset_len.offset)).await?;

              //     let mut buf = vec![0; offset_len.length as usize];
              //     reader.read_exact(&mut buf).await?;

              //     Ok(buf)
              // }
        }
    }
}

impl TileBackend {
    /// Calculate a hash from either the vec of bytes or the file at the path
    fn calculate_hash(&self) -> Result<u64> {
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
                // hasher.update_mmap(path)?;
                hasher.update_mmap_rayon(path)?;

                let finalised_bytes = hasher.finalize();
                let bytes = finalised_bytes.as_bytes();

                // Take the first 8 bytes from the hash and convert them to a u64
                Ok(u64::from_le_bytes(bytes[0..8].try_into()?))
            } // Self::Remote(_, _) => Err(PmTilesError::RemoteTileHashNotSupported.into()),
        }
    }

    fn len(&self) -> Result<u32> {
        match self {
            Self::InMemory(data) => Ok(u32::try_from(data.len())?),
            Self::OnDisk(path) => {
                let metadata = std::fs::metadata(path)?;
                Ok(u32::try_from(metadata.len())?)
            } // Self::Remote(_, offset_len) => Ok(offset_len.length),
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
