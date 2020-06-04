mod stip {
    tonic::include_proto!("stip");
}

pub use stip::*;
pub use stip::album_management_client::AlbumManagementClient;
pub use stip::album_management_server::{AlbumManagement, AlbumManagementServer};
pub use stip::data_management_client::DataManagementClient;
pub use stip::data_management_server::{DataManagement, DataManagementServer};
pub use stip::node_management_client::NodeManagementClient;
pub use stip::node_management_server::{NodeManagement, NodeManagementServer};
pub use stip::task_management_client::TaskManagementClient;
pub use stip::task_management_server::{TaskManagement, TaskManagementServer};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
