use cid::Cid;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, StreamExt};
use rs_car::{CarDecodeError, CarReader};

use crate::pb::FlatUnixFs;

#[derive(Debug)]
pub enum ReadSingleFileError {
    IoError(std::io::Error),
    CarDecodeError(CarDecodeError),
    NotSingleRoot { roots: Vec<Cid> },
    UnexpectedHeaderRoots { expected: Cid, actual: Cid },
    UnknownCid(Cid),
    DuplicateCid(Cid),
    UnexpectedCid { expected: Cid, actual: Cid },
    InvalidUnixFs(String),
    InvalidUnixFsHash(String),
}

pub async fn read_single_file<R: AsyncRead + Send + Unpin, W: AsyncWrite + Unpin>(
    car_input: &mut R,
    out: &mut W,
    root_cid: Option<&Cid>,
) -> Result<(), ReadSingleFileError> {
    let mut streamer = CarReader::new(car_input, true).await?;

    let header_root = if streamer.header.roots.len() == 1 {
        streamer
            .header
            .roots
            .get(0)
            .expect("roots has len 1 but no item at index 0")
    } else {
        return Err(ReadSingleFileError::NotSingleRoot {
            roots: streamer.header.roots,
        });
    };

    // Optional verification of the root_cid
    let root_cid = match root_cid {
        Some(root_cid) => {
            if header_root != root_cid {
                return Err(ReadSingleFileError::UnexpectedHeaderRoots {
                    expected: root_cid.clone(),
                    actual: header_root.clone(),
                });
            } else {
                root_cid
            }
        }
        None => header_root,
    };

    // Sorted links by original file layout
    let mut links_sorted: Vec<Cid> = vec![*root_cid];
    // Pointer to "consumed" links in links_sorted
    let mut links_sorted_ptr: usize = 0;

    // Can the same data block be referenced multiple times? Say in a file with lots of duplicate content

    while let Some(item) = streamer.next().await {
        let (cid, block) = item?;

        let inner = FlatUnixFs::try_from(block.as_slice())
            .map_err(|err| ReadSingleFileError::InvalidUnixFs(err.to_string()))?;

        if inner.links.len() == 0 {
            // Leaf data node, expected to be sorted
            // TODO: Cache leaf nodes until finding the first
            let expected_first_cid = links_sorted
                .get(links_sorted_ptr)
                .expect(&format!("no link at index {links_sorted_ptr}"));

            if expected_first_cid != &cid {
                return Err(ReadSingleFileError::UnexpectedCid {
                    expected: *expected_first_cid,
                    actual: cid,
                });
            }

            links_sorted_ptr += 1;
            // file_data.extend_from_slice(inner.data.Data.unwrap().as_ref());
            out.write_all(inner.data.Data.unwrap().as_ref()).await?
        } else {
            let mut links_cid = Vec::with_capacity(inner.links.len());
            for link in inner.links.iter() {
                links_cid.push(hash_to_cid(link.Hash.as_ref().expect("no Hash property"))?);
            }

            // Replace for existing link
            let index = links_sorted
                .iter()
                .position(|&r| r == cid)
                .ok_or(ReadSingleFileError::UnknownCid(cid))?;

            if index < links_sorted_ptr {
                return Err(ReadSingleFileError::DuplicateCid(cid));
            }

            links_sorted.splice(index..index + 1, links_cid);
        };
    }

    Ok(())
}

fn hash_to_cid(hash: &[u8]) -> Result<Cid, ReadSingleFileError> {
    Cid::try_from(hash).map_err(|err| ReadSingleFileError::InvalidUnixFsHash(err.to_string()))
}

impl From<CarDecodeError> for ReadSingleFileError {
    fn from(error: CarDecodeError) -> Self {
        match error {
            CarDecodeError::IoError(err) => ReadSingleFileError::IoError(err),
            err => ReadSingleFileError::CarDecodeError(err),
        }
    }
}

impl From<std::io::Error> for ReadSingleFileError {
    fn from(error: std::io::Error) -> Self {
        ReadSingleFileError::IoError(error)
    }
}
