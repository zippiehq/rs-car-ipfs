use async_std::io::ReadExt;
use futures::io::Cursor;
use rs_car_ipfs::single_file::{read_single_file_buffer, read_single_file_seek};
use std::env;
use std::{fs, path::PathBuf};

const TEST_DATA_DIR: &str = "tests/data";

#[async_std::test]
async fn read_single_file_test_data() {
    let run_only = env::var("RUN_ONLY");

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
        .filter(|path| !is_car_filepath(path))
        .collect::<Vec<&PathBuf>>();

    for non_car_filepath in non_car_filepaths.iter() {
        let expected_data_hex = read_file_to_end_hex(non_car_filepath).await;

        let input_filepaths = all_filepaths
            .iter()
            .filter(|path| is_car_filepath(path) && path_starts_with(path, non_car_filepath))
            .collect::<Vec<&PathBuf>>();

        for input_filepath in input_filepaths {
            if let Ok(run_only) = &run_only {
                if !input_filepath.to_str().unwrap().contains(run_only) {
                    continue;
                }
            }

            {
                let mut car_input = async_std::fs::File::open(input_filepath).await.unwrap();
                let mut out = Cursor::new(Vec::new());

                match read_single_file_buffer(&mut car_input, &mut out, None, None).await {
                    Err(err) => panic!(
                        "read_single_file_buffer error on {}: {:?}",
                        input_filepath.display(),
                        err,
                    ),
                    Ok(_) => {
                        assert_eq!(
                            hex::encode(out.get_ref()),
                            expected_data_hex,
                            "Different out data {}",
                            input_filepath.display()
                        );
                        println!("OK {} read_single_file_buffer", input_filepath.display())
                    }
                }
            }

            {
                let mut car_input = async_std::fs::File::open(input_filepath).await.unwrap();
                let mut out = Cursor::new(Vec::new());

                match read_single_file_seek(&mut car_input, &mut out, None).await {
                    Err(err) => panic!(
                        "read_single_file_seek error on {}: {:?}",
                        input_filepath.display(),
                        err,
                    ),
                    Ok(_) => {
                        let out_hex = hex::encode(out.get_ref());
                        assert_eq!(
                            out_hex.len(),
                            expected_data_hex.len(),
                            "Different out data len {}",
                            input_filepath.display()
                        );
                        assert_eq!(
                            hex::encode(out.get_ref()),
                            expected_data_hex,
                            "Different out data {}",
                            input_filepath.display()
                        );
                        println!("OK {} read_single_file_seek", input_filepath.display())
                    }
                }
            }
        }
    }
}

async fn read_file_to_end_hex(path: &PathBuf) -> String {
    let mut data = vec![];
    let mut file = async_std::fs::File::open(path).await.unwrap();
    file.read_to_end(&mut data).await.unwrap();
    hex::encode(data)
}

fn path_starts_with(path: &PathBuf, starts_with_path: &PathBuf) -> bool {
    path.to_str()
        .unwrap()
        .starts_with(starts_with_path.to_str().unwrap())
}

fn is_car_filepath(filepath: &PathBuf) -> bool {
    filepath.extension().map(|ext| ext.to_str().unwrap()) == Some("car")
}
