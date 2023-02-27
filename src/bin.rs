use async_std::io::{stdin, stdout};
use rs_car_ipfs::single_file::read_single_file_buffered;

#[async_std::main]
async fn main() {
    let mut stdin = stdin();
    let mut stdout = stdout();

    read_single_file_buffered(&mut stdin, &mut stdout, None, None)
        .await
        .unwrap();
}
