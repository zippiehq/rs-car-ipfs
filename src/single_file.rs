use cid::Cid;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, StreamExt};
use rs_car::CarReader;

use crate::pb::FlatUnixFs;

#[derive(Debug)]
pub enum ReadSingleFileError {
    NotSingleRoot { roots: Vec<Cid> },
    UnexpectedHeaderRoots { expected: Cid, actual: Cid },
    UnknownCid(Cid),
    DuplicateCid(Cid),
    UnexpectedCid { expected: Cid, actual: Cid },
}

pub async fn read_single_file<R: AsyncRead + Send + Unpin, W: AsyncWrite + Unpin>(
    car_input: &mut R,
    out: &mut W,
    root_cid: Option<&Cid>,
) -> Result<(), ReadSingleFileError> {
    let mut streamer = CarReader::new(car_input, true).await.unwrap();

    let header_root = if streamer.header.roots.len() == 1 {
        streamer.header.roots.get(0).unwrap()
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
        let (cid, block) = item.unwrap();

        let inner = FlatUnixFs::try_from(block.as_slice()).unwrap();

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
            out.write_all(inner.data.Data.unwrap().as_ref())
                .await
                .unwrap();
        } else {
            let links_cid = inner
                .links
                .iter()
                .map(|link| hash_to_cid(link.Hash.as_ref().unwrap()))
                .collect::<Vec<Cid>>();

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

fn hash_to_cid(hash: &[u8]) -> Cid {
    Cid::try_from(hash).unwrap()
}
