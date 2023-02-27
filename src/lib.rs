use ipfs_unixfs::file::adder::FileAdder;
use std::io::{BufRead, BufReader, Read};

mod pb;
pub mod single_file;

/// # Example
/// ```
/// compute_blocks_of_file(std::io::stdin());
/// ```
#[allow(dead_code)]
fn compute_blocks_of_file<R: Read>(r: R) {
    // CarDecodeBlockStreamer::new(r, validate_block_hash)

    let mut adder = FileAdder::default();
    let mut input = BufReader::with_capacity(adder.size_hint(), r);

    let blocks = loop {
        match input.fill_buf().unwrap() {
            x if x.is_empty() => {
                eprintln!("finishing");
                eprintln!("{:?}", adder);
                break adder.finish();
            }
            x => {
                let mut total = 0;

                while total < x.len() {
                    let (_blocks, consumed) = adder.push(&x[total..]);
                    total += consumed;
                }

                assert_eq!(total, x.len());
                input.consume(total);
            }
        }
    };

    for block in blocks {
        println!("{:?}", block.0.to_string());
    }
}
