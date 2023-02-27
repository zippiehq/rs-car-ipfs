//! Wrapper for [rs-car](https://crates.io/crates/rs-car) to read files from IPFS trustless gateways with an async API.
//!
//! # Usage
//!
//! - To read a single file buffering the block dag [`single_file::read_single_file_buffered`]
//!

mod pb;
pub mod single_file;
