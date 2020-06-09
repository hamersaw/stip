use protobuf::{self, Task, TaskBroadcastReply, TaskBroadcastRequest, TaskBroadcastType, TaskListReply, TaskListRequest, TaskManagement, TaskManagementClient};
use swarm::prelude::Dht;
use tonic::{Code, Request, Response, Status};

use crate::task::{TaskManager, TaskStatus};

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct TaskManagementImpl {
    dht: Arc<RwLock<Dht>>,
    task_manager: Arc<RwLock<TaskManager>>,
}

impl TaskManagementImpl {
    pub fn new(dht: Arc<RwLock<Dht>>, 
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

        // copy valid dht nodes
        let mut dht_nodes = Vec::new();
        {
            let dht = self.dht.read().unwrap();
            for (node_id, addrs) in dht.iter() {
                // check if rpc address is populated
                if let None = addrs.1 {
                    continue;
                }

                dht_nodes.push((*node_id, addrs.1.unwrap()));
            }
        }

        // send broadcast message to each dht node
        let mut list_replies = HashMap::new();

        for (node_id, addr) in dht_nodes {
            // initialize grpc client
            let mut client = match TaskManagementClient::connect(
                    format!("http://{}", addr)).await {
                Ok(client) => client,
                Err(e) => return Err(Status::new(Code::Unavailable,
                    format!("connection to {} failed: {}", addr, e))),
            };

            // execute message at dht node
            match TaskBroadcastType::from_i32(request.message_type).unwrap() {
                TaskBroadcastType::TaskList => {
                    let reply = match client.list(request
                            .list_request.clone().unwrap()).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("list broadcast failed: {}", e))),
                    };
                    list_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
            };
        }

        // initialize reply
        let reply = TaskBroadcastReply {
            message_type: request.message_type,
            list_replies: list_replies,
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
                let task_handle = task_handle.read().unwrap();
                
                // compile task status
                let status = match task_handle.get_status() {
                    TaskStatus::Complete =>
                        protobuf::TaskStatus::Complete,
                    TaskStatus::Failure(_) =>
                        protobuf::TaskStatus::Failure,
                    TaskStatus::Running =>
                        protobuf::TaskStatus::Running,
                };

                // initialize task protobuf
                tasks.push(Task {
                    id: *task_id,
                    items_completed: task_handle.get_items_completed(),
                    items_skipped: task_handle.get_items_skipped(),
                    items_total: task_handle.get_items_total(),
                    status: status as i32,
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
