use async_std::io::{stdin, stdout};
use rs_car::CarReader;
use rs_ipfs_car::single_file::read_single_file;

#[async_std::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = stdin();

    read_single_file(stdin).await.unwrap();
    let mut car_reader = CarReader::new(&mut r, true).await?;
    println!("{:?}", car_reader.header);

    while let Some(item) = car_reader.next().await {
        let (cid, block) = item?;
        println!("{:?} {} bytes", cid, block.len());
    }

    Ok(())
}
