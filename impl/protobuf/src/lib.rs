mod mickey {
    tonic::include_proto!("mickey");
}

pub use mickey::{InitDatasetRequest, InitDatasetReply};
pub use mickey::data_management_client::DataManagementClient;
pub use mickey::data_management_server::{DataManagement, DataManagementServer};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
