use byteorder::{ReadBytesExt, WriteBytesExt};
use comm::StreamHandler;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use st_image::StImage;

use crate::data::DataManager;

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
    data_manager: Arc<DataManager>,
}

impl TransferStreamHandler {
    pub fn new(data_manager: Arc<DataManager>) -> TransferStreamHandler {
        TransferStreamHandler {
            data_manager: data_manager,
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
                let spacecraft_len = stream.read_u8()?;
                let mut spacecraft_buf = vec![0u8; spacecraft_len as usize];
                stream.read_exact(&mut spacecraft_buf)?;
                let spacecraft_id = String::from_utf8(spacecraft_buf)?;

                let product_len = stream.read_u8()?;
                let mut product_buf = vec![0u8; product_len as usize];
                stream.read_exact(&mut product_buf)?;
                let product_id = String::from_utf8(product_buf)?;

                // read image
                let st_image = StImage::read(stream)?;

                // write image using DataManager
                self.data_manager.write_image(&spacecraft_id,
                    &product_id, &st_image)?;
            },
            None => return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported operation type '{}'", op_type)))),
        }

        Ok(())
    }
}

pub fn send_image(spacecraft_id: &str, product_id: &str, st_image: &StImage,
        addr: &SocketAddr) -> Result<(), Box<dyn Error>> {
    // open connection
    let mut stream = TcpStream::connect(addr)?;

    // write metadata
    stream.write_u8(spacecraft_id.len() as u8)?;
    stream.write(spacecraft_id.as_bytes())?;

    stream.write_u8(product_id.len() as u8)?;
    stream.write(product_id.as_bytes())?;

    // write image
    stream.write_u8(TransferOp::Write as u8)?;
    st_image.write(&mut stream)
}
