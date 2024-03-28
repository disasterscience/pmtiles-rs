use thiserror::Error;

#[derive(Error, Debug)]
/// An error which occurred within the present crate
#[allow(clippy::module_name_repetitions)]
pub enum PmTilesError {
    /// A tile was not found after a lookup
    #[error("tile not found after lookup")]
    MissingTileFollowingLookup,

    /// Unable to perform compression operation without a compression scheme
    #[error("a required compression scheme wasn't set")]
    CompressionSchemeNotSet,
}
