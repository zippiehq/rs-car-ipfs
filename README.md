# rs-ipfs-car

Wrapper for [rs-car](https://crates.io/crates/rs-car) to read files from IPFS trustless gateways with an async API

# lib usage

- To read a single file buffering the block dag [`single_file::read_single_file_buffer`]
- To read a single file without buffering the block dag [`single_file::read_single_file_seek`]

# bin usage

```
cargo install rs-car-ipfs
```

`car-ipfs` reads a CAR stream from stdin and outputs the file contents to stdout

```
curl "http://localhost:8080/ipfs/QmV3q6mo8oxf2GBuvR7zx7ABFBNP5VrRs3sCr63HQ7kEFC?format=car" | car-ipfs
```

# Roadmap

- [x] Read CAR for single file buffering all blocks in memory
- [ ] Read CAR for multiple files and directories and write to disk
