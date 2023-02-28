//! CAR stream does not include duplicated blocks, so to reconstruct a unixfs file,
//! data does not follow the same layout as the expected target file. To recreate
//! the file one must have the ability to read from arbitrary locations of the stream.
//!
//! The seek module achieves this by requiring the `out` writer to also be `AsyncSeek + AsyncRead`
//! so that it duplicated data is found it can read from itself.
//!
//! See example below of a CAR stream with de-duplicated nodes:
//!
//! Target file chunked, uppercase letters = link nodes, lowercase letters = data nodes
//! ```n
//! [ROOT                              ]
//! [X         ][Y         ][X         ]
//! [a][b][a][a][b][c][d][a][a][b][a][a]
//! ```
//!
//! Car stream layout, indexes represent time steps in the CAR stream read to match below.
//! ```n
//! 1     2  3  4  5  6  7
//! [ROOT][X][a][b][Y][c][d]
//! ```
//!
//! Representation of the "link stack". Replacing a node with `[-]` represents writing its data to out.
//! For example at step 4, when `[b]` is recieved that node is written to out + immediately consecutive
//! nodes `[a][a]` are already known so those are written too.
//!
//! ```n
//! 0 [ROOT]
//! 1 [X][Y][X]
//! 2 [a][b][a][a][Y][a][b][a][a]
//! 3 [-][b][a][a][Y][a][b][a][a]
//! 4 [-][-][-][-][Y][a][b][a][a]
//! 5 [-][-][-][-][-][c][d][a][a][b][a][a]
//! 6 [-][-][-][-][-][-][d][a][a][b][a][a]
//! 7 [-][-][-][-][-][-][-][-][-][-][-][-]
//! ```

use cid::Cid;
use futures::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, StreamExt,
};
use rs_car::CarReader;
use std::{collections::HashMap, io::SeekFrom};

use crate::pb::{FlatUnixFs, UnixFsType};

use super::{
    util::{assert_header_single_file, links_to_cids},
    ReadSingleFileError,
};

/// Read CAR stream from `car_input` as a single file without buffering the block dag in memory,
/// reading de-duplicated blocks from `out`.
///
/// # Examples
///
/// ```
/// use rs_car_ipfs::{Cid, single_file::read_single_file_seek};
/// use futures::io::Cursor;
///
/// #[async_std::main]
/// async fn main() {
///   let mut input = async_std::fs::File::open("tests/example.car").await.unwrap();
///   let mut out = Cursor::new(Vec::new());
///   let root_cid = Cid::try_from("QmUU2HcUBVSXkfWPUc3WUSeCMrWWeEJTuAgR9uyWBhh9Nf").unwrap();
///
///   read_single_file_seek(&mut input, &mut out, Some(&root_cid)).await.unwrap();
/// }
/// ```
pub async fn read_single_file_seek<
    R: AsyncRead + Send + Unpin,
    W: AsyncSeek + AsyncRead + AsyncWrite + Unpin,
>(
    car_input: &mut R,
    out: &mut W,
    root_cid: Option<&Cid>,
) -> Result<(), ReadSingleFileError> {
    let mut streamer = CarReader::new(car_input, true).await?;

    // Optional verification of the root_cid
    let root_cid = assert_header_single_file(&streamer.header, root_cid)?;

    // In-memory buffer of data nodes
    let mut nodes = HashMap::new();
    let mut sorted_links = SortedLinks::new(root_cid);
    let mut file_ptr = 0;

    // Can the same data block be referenced multiple times? Say in a file with lots of duplicate content

    while let Some(item) = streamer.next().await {
        let (cid, block) = item?;

        let inner = FlatUnixFs::try_from(block.as_slice())
            .map_err(|err| ReadSingleFileError::InvalidUnixFs(err.to_string()))?;

        // Check that the root CID is a file for sanity
        if cid == root_cid && inner.data.Type != UnixFsType::File {
            return Err(ReadSingleFileError::RootCidIsNotFile);
        }

        let node = if inner.links.len() == 0 {
            // Leaf data node
            // - Only write nodes that are the next possible write
            // - If the CID of the data node is not known, discard
            // - If the CID of the node is known but is not the first, error
            match sorted_links.find(cid) {
                FindResult::IsNext => {} // Ok
                FindResult::NotNext => return Err(ReadSingleFileError::DataNodesNotSorted),
                FindResult::Unknown => continue,
            }

            let data = inner.data.Data.unwrap();

            // Write data now, and keep a record for potential future writes
            out.write_all(&data).await?;

            // Wrote `cid` advance write ptr and sorted links pointer
            let size = data.len();
            let start = file_ptr;
            file_ptr += size;
            sorted_links.advance()?;

            UnixFsNode::Data { start, size }
        } else {
            // Intermediary node (links)
            UnixFsNode::Links(links_to_cids(inner.links)?)
        };

        nodes.insert(cid, node);

        // Attempt to progress on potential pending nodes
        while let Some(first) = sorted_links.first() {
            match nodes.get(first) {
                // Next node in the file layout is an existing node of already written data.
                // Use AsyncSeek to read from disk and write into new location
                Some(UnixFsNode::Data { start, size }) => {
                    copy_from_to_itself(out, *start, file_ptr, *size).await?;

                    // Wrote `cid` advance write ptr and sorted links pointer
                    file_ptr += size;
                    sorted_links.advance()?;
                }
                // Next node in the file layout is an existing links node, apply insert_replace
                Some(UnixFsNode::Links(links)) => {
                    sorted_links.insert_replace(&first.clone(), links.clone())
                }
                // Next node is not yet known, continue
                None => break,
            }
        }
    }

    match sorted_links.remaining() {
        Some(links) => return Err(ReadSingleFileError::PendingLinksAtEOF(links.to_vec())),
        None => Ok(()),
    }
}

struct SortedLinks<T: PartialEq + Clone> {
    pub sorted_items: Vec<T>,
    items_ptr: usize,
}

impl<T: PartialEq + Clone> SortedLinks<T> {
    fn new(root: T) -> Self {
        Self {
            sorted_items: vec![root],
            items_ptr: 0,
        }
    }

    fn find(&self, item: T) -> FindResult {
        // TODO: Optimize with a Set if necessary
        match self
            .sorted_items
            .iter()
            .skip(self.items_ptr)
            // Note: position index is relative to the skipped elements
            .position(|x| *x == item)
        {
            Some(0) => FindResult::IsNext,
            Some(_) => FindResult::NotNext,
            None => FindResult::Unknown,
        }
    }

    fn first(&self) -> Option<&T> {
        self.sorted_items.get(self.items_ptr)
    }

    fn advance(&mut self) -> Result<(), ReadSingleFileError> {
        // items_ptr max value is the Vec len() to signal that all items are consumed
        if self.items_ptr >= self.sorted_items.len() {
            return Err(ReadSingleFileError::InternalError(
                "attempting to increase items_ptr beyond items length".to_string(),
            ));
        }

        self.items_ptr += 1;

        Ok(())
    }

    fn remaining(&self) -> Option<&[T]> {
        if self.items_ptr >= self.sorted_items.len() {
            None
        } else {
            Some(self.sorted_items.split_at(self.items_ptr).1)
        }
    }

    fn insert_replace(&mut self, root: &T, children: Vec<T>) {
        // Search match on a loop since array in mutated on splice
        if let Some(index) = self.sorted_items.iter().position(|x| x == root) {
            self.sorted_items.splice(index..index + 1, children);
        }
    }
}

enum FindResult {
    IsNext,
    NotNext,
    Unknown,
}

enum UnixFsNode {
    Links(Vec<Cid>),
    Data { start: usize, size: usize },
}

async fn copy_from_to_itself<W: AsyncSeek + AsyncRead + AsyncWrite + Unpin>(
    r: &mut W,
    src_offset: usize,
    dest_offset: usize,
    size: usize,
) -> Result<(), std::io::Error> {
    r.seek(SeekFrom::Start(src_offset as u64)).await?;

    let mut buffer = vec![0; size];
    r.read_exact(&mut buffer).await?;

    r.seek(SeekFrom::Start(dest_offset as u64)).await?;

    r.write_all(&buffer).await?;

    Ok(())
}
