use protobuf::{LoadDirectoryRequest, LoadDirectoryReply, TaskListRequest, TaskListReply, TaskShowRequest, TaskShowReply, DataManagement};
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

    async fn task_list(&self, request: Request<TaskListRequest>)
            -> Result<Response<TaskListReply>, Status> {
        trace!("TaskListRequest: {:?}", request);

        let reply = TaskListReply {
        };

        Ok(Response::new(reply))
    }

    async fn task_show(&self, request: Request<TaskShowRequest>)
            -> Result<Response<TaskShowReply>, Status> {
        trace!("TaskShowRequest: {:?}", request);

        let reply = TaskShowReply {
            completion_percentage: 0.0, // TODO - fix
        };

        Ok(Response::new(reply))
    }
}
