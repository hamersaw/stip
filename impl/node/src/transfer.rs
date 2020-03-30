use byteorder::{ReadBytesExt, WriteBytesExt};
use comm::StreamHandler;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use st_image::StImage;

use std::error::Error;
use std::net::{TcpStream, SocketAddr};

#[derive(FromPrimitive)]
enum TransferOp {
    Read = 0,
    Write = 1,
}

pub struct TransferStreamHandler {
}

impl TransferStreamHandler {
    pub fn new() -> TransferStreamHandler {
        TransferStreamHandler {
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
                let st_image = StImage::read(stream)?;

                // TODO - process st_image
                println!("received image with geohash '{:?}'",
                    st_image.geohash());
            },
            None => return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported operation type '{}'", op_type)))),
        }

        Ok(())
    }
}

pub fn send_image(st_image: &StImage, 
        addr: &SocketAddr) -> Result<(), Box<dyn Error>> {
    // open connection
    let mut stream = TcpStream::connect(addr)?;

    // write image
    stream.write_u8(TransferOp::Write as u8)?;
    st_image.write(&mut stream)
}
