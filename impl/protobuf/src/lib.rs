mod mickey {
    tonic::include_proto!("mickey");
}

pub use mickey::*;
pub use mickey::cluster_management_client::ClusterManagementClient;
pub use mickey::cluster_management_server::{ClusterManagement, ClusterManagementServer};
pub use mickey::data_management_client::DataManagementClient;
pub use mickey::data_management_server::{DataManagement, DataManagementServer};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
