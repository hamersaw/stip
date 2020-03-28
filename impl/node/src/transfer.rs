use comm::StreamHandler;

use std::net::TcpStream;

pub struct TransferStreamHandler {
}

impl TransferStreamHandler {
    pub fn new() -> TransferStreamHandler {
        TransferStreamHandler {
        }
    }
}

impl StreamHandler for TransferStreamHandler {
    fn process(&self, _: &mut TcpStream) -> std::io::Result<()> {
        Ok(())
    }
}
