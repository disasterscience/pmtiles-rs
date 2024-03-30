# PMTiles (for Rust)

A low level implementation of [the `PMTiles` format](https://github.com/protomaps/PMTiles) refactored for Tokio and write workloads.

## Be ware of dragons 🐲

Originally forked from [pmtiles-rs, pmtiles2 crate](https://github.com/arma-place/pmtiles-rs/). Open to upstreaming, but there's quite few discretionary changes largely to improve DX, performance and some async bug fixes. It also doesn't work rn 🥲.

### Bench Rough Notes

Test case: Loading 50GB, 500k XYZ tiles (zoom level 0-18) from 10Gbps NVME drive. In debug mode 😱.

Blake3 update_mmap_rayon: load avg: 2,93, read average 15-16MB/s / Before Futures - 50 minutes to read

Blake3 update_mmap: load avg: 1.50, read average 7-8MB/s / Before Futures - Slower, and disconnected the drive :o

Blake3 update_mmap_rayon: load avg: 30, read average 110MB/s / After, with concurrent read futures - 6 minutes to read.
