use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use comm::StreamHandler;
use gdal::raster::Dataset;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use crate::album::AlbumManager;

use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpStream, SocketAddr};
use std::sync::{Arc, RwLock};

#[derive(FromPrimitive)]
enum TransferOp {
    ReadImage = 0,
    WriteImage = 1,
}

pub struct TransferStreamHandler {
    album_manager: Arc<RwLock<AlbumManager>>,
}

impl TransferStreamHandler {
    pub fn new(album_manager: Arc<RwLock<AlbumManager>>)
            -> TransferStreamHandler {
        TransferStreamHandler {
            album_manager: album_manager,
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
                // read everything
                let album = read_string(stream)?;
                let mut dataset = st_image::prelude::read(stream)?;
                let geocode = read_string(stream)?;
                let pixel_coverage = stream.read_f64::<BigEndian>()?;
                let platform = read_string(stream)?;
                let source = read_string(stream)?;
                let subdataset = stream.read_u8()?;
                let tile = read_string(stream)?;
                let timestamp = stream.read_i64::<BigEndian>()?;

                // write image using AlbumManager
                let album_manager = self.album_manager.read().unwrap();
                match album_manager.get(&album) {
                    Some(album) => {
                        let mut album = album.write().unwrap();
                        album.write(&mut dataset, &geocode,
                            pixel_coverage, &platform, &source,
                            subdataset, &tile, timestamp)?;
                    },
                    None => warn!("album '{}' does not exist", album),
                }
                /*let mut image_manager =
                    self.image_manager.write().unwrap();
                image_manager.write(&mut dataset, &geocode,
                    pixel_coverage, &platform, &source,
                    subdataset, &tile, timestamp)?;*/

                // write success
                stream.write_u8(1)?;
            },
            None => return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported operation type '{}'", op_type)))),
        }

        Ok(())
    }
}

pub fn read_string<T: Read>(reader: &mut T)
        -> Result<String, Box<dyn Error>> {
    let len = reader.read_u8()?;
    let mut buf = vec![0u8; len as usize];
    reader.read_exact(&mut buf)?;
    Ok(String::from_utf8(buf)?)
}

pub fn send_image(addr: &SocketAddr, album: &str, dataset: &Dataset,
        geocode: &str, pixel_coverage: f64, platform: &str,
        source: &str, subdataset: u8, tile: &str, timestamp: i64)
        -> Result<(), Box<dyn Error>> {
    // open connection
    let mut stream = TcpStream::connect(addr)?;
    stream.write_u8(TransferOp::WriteImage as u8)?;

    // write everything
    write_string(&album, &mut stream)?;
    st_image::prelude::write(&dataset, &mut stream)?;
    write_string(&geocode, &mut stream)?;
    stream.write_f64::<BigEndian>(pixel_coverage)?;
    write_string(&platform, &mut stream)?;
    write_string(&source, &mut stream)?;
    stream.write_u8(subdataset)?;
    write_string(&tile, &mut stream)?;
    stream.write_i64::<BigEndian>(timestamp)?;
 
    // read success
    let _ = stream.read_u8()?;

    Ok(())
}

pub fn write_string<T: Write>(value: &str, writer: &mut T)
        -> Result<(), Box<dyn Error>> {
    writer.write_u8(value.len() as u8)?;
    writer.write(value.as_bytes())?;
    Ok(())
}
