use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use comm::StreamHandler;
use gdal::raster::Dataset;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::image::{RAW_DATASET, ImageManager};

use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpStream, SocketAddr};
use std::sync::Arc;

#[derive(FromPrimitive)]
enum TransferOp {
    Read = 0,
    Write = 1,
}

pub struct TransferStreamHandler {
    image_manager: Arc<ImageManager>,
}

impl TransferStreamHandler {
    pub fn new(image_manager: Arc<ImageManager>) -> TransferStreamHandler {
        TransferStreamHandler {
            image_manager: image_manager,
        }
    }
}

impl StreamHandler for TransferStreamHandler {
    fn process(&self, stream: &mut TcpStream) -> Result<(), Box<dyn Error>> {
        // read operation type
        let op_type = stream.read_u8()?;
        match FromPrimitive::from_u8(op_type) {
            Some(TransferOp::Read) => unimplemented!(),
            Some(TransferOp::Write) => {
                // read metadata
                let platform_len = stream.read_u8()?;
                let mut platform_buf = vec![0u8; platform_len as usize];
                stream.read_exact(&mut platform_buf)?;
                let platform = String::from_utf8(platform_buf)?;

                let geohash_len = stream.read_u8()?;
                let mut geohash_buf = vec![0u8; geohash_len as usize];
                stream.read_exact(&mut geohash_buf)?;
                let geohash = String::from_utf8(geohash_buf)?;

                let band_len = stream.read_u8()?;
                let mut band_buf = vec![0u8; band_len as usize];
                stream.read_exact(&mut band_buf)?;
                let band = String::from_utf8(band_buf)?;

                let tile_len = stream.read_u8()?;
                let mut tile_buf = vec![0u8; tile_len as usize];
                stream.read_exact(&mut tile_buf)?;
                let tile = String::from_utf8(tile_buf)?;

                let start_date = stream.read_i64::<BigEndian>()?;
                let end_date = stream.read_i64::<BigEndian>()?;

                let coverage = stream.read_f64::<BigEndian>()?;

                // read image
                let image = st_image::prelude::read(stream)?;

                // write image using ImageManager
                self.image_manager.write(&platform, &geohash,
                    &band, RAW_DATASET, &tile, start_date,
                    end_date, coverage, &image)?;
            },
            None => return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported operation type '{}'", op_type)))),
        }

        Ok(())
    }
}

pub fn send_image(platform: &str, geohash: &str, band: &str, tile: &str,
        start_date: i64, end_date: i64, coverage: f64, image: &Dataset,
        addr: &SocketAddr) -> Result<(), Box<dyn Error>> {
    // open connection
    let mut stream = TcpStream::connect(addr)?;
    stream.write_u8(TransferOp::Write as u8)?;

    // write metadata
    stream.write_u8(platform.len() as u8)?;
    stream.write(platform.as_bytes())?;

    stream.write_u8(geohash.len() as u8)?;
    stream.write(geohash.as_bytes())?;

    stream.write_u8(band.len() as u8)?;
    stream.write(band.as_bytes())?;

    stream.write_u8(tile.len() as u8)?;
    stream.write(tile.as_bytes())?;

    stream.write_i64::<BigEndian>(start_date)?;
    stream.write_i64::<BigEndian>(end_date)?;

    stream.write_f64::<BigEndian>(coverage)?;

    // write dataset
    st_image::prelude::write(&image, &mut stream)?;

    Ok(())
}
