pub mod mickey {
    tonic::include_proto!("mickey");
}

pub use mickey::{InitDatasetRequest, InitDatasetReply};
pub use mickey::mickier_client::MickierClient;
pub use mickey::mickier_server::{Mickier, MickierServer};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
