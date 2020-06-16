use protobuf::{Task, TaskClearReply, TaskClearRequest, TaskBroadcastReply, TaskBroadcastRequest, TaskBroadcastType, TaskListReply, TaskListRequest, TaskManagement, TaskManagementClient};
use swarm::prelude::Dht;
use tonic::{Code, Request, Response, Status};

use crate::task::TaskManager;

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
        let mut clear_replies = HashMap::new();
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
                TaskBroadcastType::TaskClear => {
                    let reply = match client.clear(request
                            .clear_request.clone().unwrap()).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("clear broadcast failed: {}", e))),
                    };
                    clear_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
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
                //let task_handle = task_handle.read().unwrap();
                
                // compile task status
                /*let status = match (task_handle.running(),
                        task_handle.completed_count()) {
                    (true, 0) => TaskStatus::Initializing,
                    (true, _) => TaskStatus::Running,
                    (false, x) if x < task_handle.total_count()
                        => TaskStatus::Failed,
                    (false, _) => TaskStatus::Completed,
                };*/
                /*let status = match task_handle.get_status() {
                    TaskStatus::Complete =>
                        protobuf::TaskStatus::Complete,
                    TaskStatus::Failure(_) =>
                        protobuf::TaskStatus::Failure,
                    TaskStatus::Running =>
                        protobuf::TaskStatus::Running,
                };*/

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
