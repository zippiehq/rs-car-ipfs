use rs_car::{CarHeader, Cid};

use crate::pb::PBLink;

use super::ReadSingleFileError;

pub fn assert_header_single_file(
    header: &CarHeader,
    root_cid: Option<&Cid>,
) -> Result<Cid, ReadSingleFileError> {
    Ok(match root_cid {
        Some(root_cid) => *root_cid,
        None => {
            // If not root CID is provided, assume header contains the single root_cid for this file
            if header.roots.len() == 1 {
                header.roots[0]
            } else {
                return Err(ReadSingleFileError::NotSingleRoot {
                    roots: header.roots.clone(),
                });
            }
        }
    })
}

pub fn links_to_cids<'a>(links: Vec<PBLink<'a>>) -> Result<Vec<Cid>, ReadSingleFileError> {
    let mut links_cid = Vec::with_capacity(links.len());
    for link in links.iter() {
        links_cid.push(hash_to_cid(
            link.Hash
                .as_ref()
                .ok_or(ReadSingleFileError::PBLinkHasNoHash)?,
        )?);
    }
    Ok(links_cid)
}

fn hash_to_cid(hash: &[u8]) -> Result<Cid, ReadSingleFileError> {
    Cid::try_from(hash).map_err(|err| ReadSingleFileError::InvalidUnixFsHash(err.to_string()))
}
