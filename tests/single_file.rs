use futures::io::Cursor;
use rs_ipfs_car::single_file::read_single_file;
use std::{error::Error, ffi::OsStr, fs, path::PathBuf};

const TEST_DATA_DIR: &str = "tests/data";

#[async_std::test]
async fn read_single_file_test_data() {
    let all_filepaths = fs::read_dir(TEST_DATA_DIR)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_file() {
                Some(path)
            } else {
                None
            }
        })
        .collect::<Vec<PathBuf>>();

    let non_car_filepaths = all_filepaths
        .iter()
        .filter(|path| path.extension().map(|path| path.to_str().unwrap()) != Some("car"))
        .collect::<Vec<&PathBuf>>();

    for non_car_filepath in non_car_filepaths.iter() {
        let input_filepaths = all_filepaths
            .iter()
            .filter(|path| path.starts_with(non_car_filepath))
            .collect::<Vec<&PathBuf>>();

        for input_filepath in input_filepaths {
            let mut car_input = async_std::fs::File::open(input_filepath).await.unwrap();
            let mut out = Cursor::new(Vec::new());

            match read_single_file(&mut car_input, &mut out, None).await {
                Err(err) => panic!(
                    "read_single_file error on {}: {:?}",
                    input_filepath.display(),
                    err,
                ),
                Ok(_) => {
                    println!("OK {}", input_filepath.display())
                }
            }
        }
    }

    println!("{:?}", all_filepaths);
    println!("{:?}", non_car_filepaths);
}
