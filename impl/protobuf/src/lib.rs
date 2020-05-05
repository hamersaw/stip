mod stip {
    tonic::include_proto!("stip");
}

pub use stip::*;
pub use stip::cluster_management_client::ClusterManagementClient;
pub use stip::cluster_management_server::{ClusterManagement, ClusterManagementServer};
pub use stip::data_management_client::DataManagementClient;
pub use stip::data_management_server::{DataManagement, DataManagementServer};
pub use stip::task_management_client::TaskManagementClient;
pub use stip::task_management_server::{TaskManagement, TaskManagementServer};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
