use protobuf::{Task, TaskClearReply, TaskClearRequest, TaskBroadcastReply, TaskBroadcastRequest, TaskBroadcastType, TaskListReply, TaskListRequest, TaskManagement, TaskManagementClient};
use swarm::prelude::Dht;
use tonic::{Code, Request, Response, Status};

use crate::task::TaskManager;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct TaskManagementImpl {
    dht: Arc<Dht>,
    task_manager: Arc<RwLock<TaskManager>>,
}

impl TaskManagementImpl {
    pub fn new(dht: Arc<Dht>, 
            task_manager: Arc<RwLock<TaskManager>>) -> TaskManagementImpl {
        TaskManagementImpl {
            dht: dht,
            task_manager: task_manager,
        }
    }
}

#[tonic::async_trait]
impl TaskManagement for TaskManagementImpl {
    async fn broadcast(&self, request: Request<TaskBroadcastRequest>)
            -> Result<Response<TaskBroadcastReply>, Status> {
        trace!("TaskBroadcastRequest: {:?}", request);
        let request = request.get_ref();

        // send broadcast message to each dht node
        let mut clear_replies = HashMap::new();
        let mut list_replies = HashMap::new();

        for node in self.dht.nodes() {
            // get rpc address
            let addr = format!("http://{}:{}", node.get_ip_address(),
                node.get_metadata("rpc_port").unwrap());

            // initialize grpc client
            let mut client = match TaskManagementClient::connect(
                    addr.clone()).await {
                Ok(client) => client,
                Err(e) => return Err(Status::new(Code::Unavailable,
                    format!("connection to {} failed: {}", addr, e))),
            };

            // execute message at dht node
            match TaskBroadcastType::from_i32(request.message_type).unwrap() {
                TaskBroadcastType::TaskClear => {
                    let reply = match client.clear(request
                            .clear_request.clone().unwrap()).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("clear broadcast failed: {}", e))),
                    };
                    clear_replies.insert(node.get_id(),
                        reply.get_ref().to_owned());
                },
                TaskBroadcastType::TaskList => {
                    let reply = match client.list(request
                            .list_request.clone().unwrap()).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("list broadcast failed: {}", e))),
                    };
                    list_replies.insert(node.get_id(),
                        reply.get_ref().to_owned());
                },
            };
        }

        // initialize reply
        let reply = TaskBroadcastReply {
            message_type: request.message_type,
            clear_replies: clear_replies,
            list_replies: list_replies,
        };

        Ok(Response::new(reply))
    }

    async fn clear(&self, request: Request<TaskClearRequest>)
            -> Result<Response<TaskClearReply>, Status> {
        trace!("TaskClearRequest: {:?}", request);

        // clear completed tasks from task_manager
        {
            let mut task_manager = self.task_manager.write().unwrap();
            if let Err(e) = task_manager.clear() {
                return Err(Status::new(Code::Unknown,
                    format!("TaskManager clear failed: {}", e)));
            }
        }

        // initialize reply
        let reply = TaskClearReply {
        };

        Ok(Response::new(reply))
    }

    async fn list(&self, request: Request<TaskListRequest>)
            -> Result<Response<TaskListReply>, Status> {
        trace!("TaskListRequest: {:?}", request);

        // populate tasks from task_manager
        let mut tasks = Vec::new();
        {
            let task_manager = self.task_manager.read().unwrap();
            for (task_id, task_handle) in task_manager.iter() {
                // initialize task protobuf
                tasks.push(Task {
                    completed_count: task_handle.completed_count(),
                    id: *task_id,
                    running: task_handle.running(),
                    skipped_count: task_handle.skipped_count(),
                    total_count: task_handle.total_count(),
                });
            }
        }

        // initialize reply
        let reply = TaskListReply {
            tasks: tasks,
        };

        Ok(Response::new(reply))
    }
}
