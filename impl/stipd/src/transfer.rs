use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use comm::StreamHandler;
use gdal::raster::Dataset;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::image::ImageManager;

use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpStream, SocketAddr};
use std::sync::{Arc, RwLock};

#[derive(FromPrimitive)]
enum TransferOp {
    ReadImage = 0,
    WriteImage = 1,
    ReadMetadata = 2,
    WriteMetadata = 3,
}

pub struct TransferStreamHandler {
    image_manager: Arc<RwLock<ImageManager>>,
}

impl TransferStreamHandler {
    pub fn new(image_manager: Arc<RwLock<ImageManager>>)
            -> TransferStreamHandler {
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
            Some(TransferOp::ReadImage) => unimplemented!(),
            Some(TransferOp::WriteImage) => {
                // read metadata
                let platform = read_string(stream)?;
                let geohash = read_string(stream)?;
                let source = read_string(stream)?;
                let tile = read_string(stream)?;
                let timestamp = stream.read_i64::<BigEndian>()?;
                let pixel_coverage = stream.read_f64::<BigEndian>()?;

                // read image
                let mut dataset = st_image::prelude::read(stream)?;

                // write image using ImageManager
                let mut image_manager = self.image_manager.read().unwrap();
                image_manager.write(&platform, &geohash, &source,
                    &tile, timestamp, pixel_coverage, &mut dataset)?;
            },
            Some(TransferOp::ReadMetadata) => unimplemented!(),
            Some(TransferOp::WriteMetadata) => {
                // read metadata
                let platform = read_string(stream)?;
                let geohash = read_string(stream)?;
                let source = read_string(stream)?;
                let tile = read_string(stream)?;
                let timestamp = stream.read_i64::<BigEndian>()?;
                let pixel_coverage = stream.read_f64::<BigEndian>()?;

                // read files
                let mut files = Vec::new();
                for i in 0..stream.read_u8()? {
                    let path = read_string(stream)?;
                    let description = read_string(stream)?;
                    files.push((path, description));
                }

                // write image metadata using ImageManager
                let mut image_manager = self.image_manager.write().unwrap();
                image_manager.write_metadata(&platform, &geohash, &source,
                    &tile, timestamp, pixel_coverage, &files)?;
            },
            None => return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported operation type '{}'", op_type)))),
        }

        Ok(())
    }
}

pub fn read_string<T: Read>(reader: &mut T) -> Result<String, Box<dyn Error>> {
    let len = reader.read_u8()?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

pub fn send_image(platform: &str, geohash: &str, source: &str,
        tile: &str, timestamp: i64, pixel_coverage: f64,
        image: &Dataset, addr: &SocketAddr) -> Result<(), Box<dyn Error>> {
    // open connection
    let mut stream = TcpStream::connect(addr)?;
    stream.write_u8(TransferOp::WriteImage as u8)?;

    // write metadata
    write_string(&platform, &mut stream)?;
    write_string(&geohash, &mut stream)?;
    write_string(&source, &mut stream)?;
    write_string(&tile, &mut stream)?;
    stream.write_i64::<BigEndian>(timestamp)?;
    stream.write_f64::<BigEndian>(pixel_coverage)?;

    // write dataset
    st_image::prelude::write(&image, &mut stream)?;

    Ok(())
}

pub fn send_metadata(platform: &str, geohash: &str, source: &str,
        tile: &str, timestamp: i64, pixel_coverage: f64,
        files: &Vec<(String, String)>, addr: &SocketAddr)
        -> Result<(), Box<dyn Error>> {
    // open connection
    let mut stream = TcpStream::connect(addr)?;
    stream.write_u8(TransferOp::WriteMetadata as u8)?;

    // write metadata
    write_string(&platform, &mut stream)?;
    write_string(&geohash, &mut stream)?;
    write_string(&source, &mut stream)?;
    write_string(&tile, &mut stream)?;
    stream.write_i64::<BigEndian>(timestamp)?;
    stream.write_f64::<BigEndian>(pixel_coverage)?;

    // write files
    stream.write_u8(files.len() as u8)?;
    for (path, description) in files {
        write_string(&path, &mut stream)?;
        write_string(&description, &mut stream)?;
    }

    Ok(())
}

pub fn write_string<T: Write>(value: &str, writer: &mut T)
        -> Result<(), Box<dyn Error>> {
    writer.write_u8(value.len() as u8)?;
    writer.write(value.as_bytes())?;
    Ok(())
}
