use cid::Cid;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, StreamExt};
use rs_car::{CarDecodeError, CarReader};
use std::collections::HashMap;

use crate::pb::{FlatUnixFs, UnixFsType};

#[derive(Debug)]
pub enum ReadSingleFileError {
    IoError(std::io::Error),
    CarDecodeError(CarDecodeError),
    NotSingleRoot { roots: Vec<Cid> },
    UnexpectedHeaderRoots { expected: Cid, actual: Cid },
    InvalidUnixFs(String),
    InvalidUnixFsHash(String),
    MissingNode(Cid),
    MaxBufferedData(usize),
    RootCidIsNotFile,
}

/// Read CAR stream from `car_input`
pub async fn read_single_file_buffered<R: AsyncRead + Send + Unpin, W: AsyncWrite + Unpin>(
    car_input: &mut R,
    out: &mut W,
    root_cid: Option<&Cid>,
    max_buffer: Option<usize>,
) -> Result<(), ReadSingleFileError> {
    let mut streamer = CarReader::new(car_input, true).await?;

    // Optional verification of the root_cid
    let root_cid = match root_cid {
        Some(root_cid) => *root_cid,
        None => {
            // If not root CID is provided, assume header contains the single root_cid for this file
            if streamer.header.roots.len() == 1 {
                streamer.header.roots[0]
            } else {
                return Err(ReadSingleFileError::NotSingleRoot {
                    roots: streamer.header.roots,
                });
            }
        }
    };

    // In-memory buffer of data nodes
    let mut nodes = HashMap::new();
    let mut buffered_data_len: usize = 0;

    // Can the same data block be referenced multiple times? Say in a file with lots of duplicate content

    while let Some(item) = streamer.next().await {
        let (cid, block) = item?;

        let inner = FlatUnixFs::try_from(block.as_slice())
            .map_err(|err| ReadSingleFileError::InvalidUnixFs(err.to_string()))?;

        // Check that the root CID is a file for sanity
        if cid == root_cid {
            match inner.data.Type {
                UnixFsType::File => {} // Ok,
                _ => return Err(ReadSingleFileError::RootCidIsNotFile),
            }
        }

        if inner.links.len() == 0 {
            // Leaf data node
            // TODO: Is it possible to prevent having to copy here?
            let data = inner.data.Data.unwrap().to_vec();

            // Allow to limit max buffered data to prevent OOM
            if let Some(max_buffer) = max_buffer {
                buffered_data_len += data.len();
                if buffered_data_len > max_buffer {
                    return Err(ReadSingleFileError::MaxBufferedData(max_buffer));
                }
            }

            nodes.insert(cid, UnixFsNode::Data(data));
        } else {
            // Intermediary node (links)
            let mut links_cid = Vec::with_capacity(inner.links.len());
            for link in inner.links.iter() {
                links_cid.push(hash_to_cid(link.Hash.as_ref().expect("no Hash property"))?);
            }

            nodes.insert(cid, UnixFsNode::Links(links_cid));
        };
    }

    for data in flatten_tree(&nodes, &root_cid)? {
        out.write_all(data).await?
    }

    Ok(())
}

fn flatten_tree<'a>(
    nodes: &'a HashMap<Cid, UnixFsNode>,
    root_cid: &Cid,
) -> Result<Vec<&'a Vec<u8>>, ReadSingleFileError> {
    let node = nodes
        .get(root_cid)
        .ok_or(ReadSingleFileError::MissingNode(root_cid.clone()))?;

    Ok(match node {
        UnixFsNode::Data(data) => vec![data],
        UnixFsNode::Links(links) => {
            let mut out = vec![];
            for link in links {
                for data in flatten_tree(nodes, link)? {
                    out.push(data);
                }
            }
            out
        }
    })
}

enum UnixFsNode {
    Links(Vec<Cid>),
    Data(Vec<u8>),
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

impl std::fmt::Display for ReadSingleFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ReadSingleFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReadSingleFileError::IoError(err) => Some(err),
            ReadSingleFileError::CarDecodeError(err) => Some(err),
            _ => None,
        }
    }
}
