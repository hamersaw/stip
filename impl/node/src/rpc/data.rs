use protobuf::{LoadDirectoryRequest, LoadDirectoryReply, ProcessListRequest, ProcessListReply, ProcessShowRequest, ProcessShowReply, DataManagement};
use tonic::{Request, Response, Status};

pub struct DataManagementImpl {
}

impl DataManagementImpl {
    pub fn new() -> DataManagementImpl {
        DataManagementImpl {
        }
    }
}

#[tonic::async_trait]
impl DataManagement for DataManagementImpl {
    async fn load_directory(&self, request: Request<LoadDirectoryRequest>)
            -> Result<Response<LoadDirectoryReply>, Status> {
        trace!("LoadDirectoryRequest: {:?}", request);

        let reply = LoadDirectoryReply {
            id: 0, // TODO - fix
        };

        Ok(Response::new(reply))
    }

    async fn process_list(&self, request: Request<ProcessListRequest>)
            -> Result<Response<ProcessListReply>, Status> {
        trace!("ProcessListRequest: {:?}", request);

        let reply = ProcessListReply {
        };

        Ok(Response::new(reply))
    }

    async fn process_show(&self, request: Request<ProcessShowRequest>)
            -> Result<Response<ProcessShowReply>, Status> {
        trace!("ProcessShowRequest: {:?}", request);

        let reply = ProcessShowReply {
            completion_percentage: 0.0, // TODO - fix
        };

        Ok(Response::new(reply))
    }
}
