use protobuf::{LoadRequest, LoadReply, TaskListRequest, TaskListReply, TaskShowRequest, TaskShowReply, DataManagement};
use tonic::{Request, Response, Status};

use crate::task::TaskManager;
use crate::task::load::LoadEarthExplorerTask;

use std::sync::{Arc, RwLock};

pub struct DataManagementImpl {
    task_manager: Arc<RwLock<TaskManager>>,
}

impl DataManagementImpl {
    pub fn new(task_manager: Arc<RwLock<TaskManager>>) -> DataManagementImpl {
        DataManagementImpl {
            task_manager: task_manager,
        }
    }
}

#[tonic::async_trait]
impl DataManagement for DataManagementImpl {
    async fn load(&self, request: Request<LoadRequest>)
            -> Result<Response<LoadReply>, Status> {
        trace!("LoadDirectoryRequest: {:?}", request);

        // initialize task
        let task = LoadEarthExplorerTask::new();

        // execute task using task manager
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task).unwrap() // TODO - handle error
        };

        // initialize reply
        let reply = LoadReply {
            task_id: task_id,
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
