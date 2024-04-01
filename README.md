# PMTiles (for Rust)

A low level implementation of [the `PMTiles` format](https://github.com/protomaps/PMTiles) refactored for Tokio and write workloads.

## Be ware of dragons 🐲

Originally forked from [pmtiles-rs, pmtiles2 crate](https://github.com/arma-place/pmtiles-rs/). Open to upstreaming, but there's quite few discretionary changes largely to improve DX, performance and some async bug fixes. It also doesn't work rn 🥲.

### Bench Rough Notes

Test case: Loading 50GB, 500k XYZ tiles (zoom level 0-18) from 10Gbps NVME drive. In debug mode 😱.

Blake3 update_mmap_rayon: load avg: 2,93, read average 15-16MB/s / Before Futures - 50 minutes to read

Blake3 update_mmap: load avg: 1.50, read average 7-8MB/s / Before Futures - Slower, and disconnected the drive :o

Blake3 update_mmap_rayon: load avg: 30, read average 110MB/s / After, with concurrent read futures - 6 minutes to read.

Without hashing and parallel load, channel for writing File, write average ~20MB/s: 22mins

[2024-04-01T02:34:48Z INFO magicgeo] Initialising magicgeo v1.0.13-rc.1
[2024-04-01T02:34:48Z INFO magicgeo] Loading z/x/y tiles from folder: "/mnt/f/act/"
[2024-04-01T02:35:16Z DEBUG pmtiles2::writer] Writing PMTiles to file: "/mnt/f/act.pmtiles"
[2024-04-01T02:56:54Z DEBUG magicgeo] Finished

Without hashing and parallel load, channel for writing (256), BufWrite, write average ~20MB/s: 23mins

[2024-04-01T03:00:17Z INFO magicgeo] Initialising magicgeo v1.0.13-rc.1
[2024-04-01T03:00:17Z INFO magicgeo] Loading z/x/y tiles from folder: "/mnt/f/act/"
[2024-04-01T03:00:48Z DEBUG pmtiles2::writer] Writing PMTiles to file: "/mnt/f/act.pmtiles"
[2024-04-01T03:23:51Z DEBUG magicgeo] Finished

Hashing small files only and parallel load, channel for writing (2048), BufWrite, write average ~26MB/s: 14mins

Add everything, no hashing, channel, BufWrite ~10-15MB/s but seems more correct?

[2024-04-01T07:56:10Z INFO magicgeo] Initialising magicgeo v1.0.13-rc.1
[2024-04-01T07:56:10Z INFO magicgeo] Loading z/x/y tiles from folder: "/mnt/f/act"
[2024-04-01T07:57:01Z INFO magicgeo] Writing to file: "/mnt/f/act.pmtiles"
[2024-04-01T08:43:37Z DEBUG magicgeo] Finished

Remove Async for writing