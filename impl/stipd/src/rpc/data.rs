use protobuf::{self, DataManagementClient, FillAllReply, FillAllRequest, FillReply, FillRequest, Image, ImageFormat as ProtoImageFormat, LoadFormat as ProtoLoadFormat, LoadReply, LoadRequest, SearchAllReply, SearchAllRequest, SearchReply, SearchRequest, SplitAllReply, SplitAllRequest, SplitReply, SplitRequest, Task, TaskListAllReply, TaskListAllRequest, TaskListReply, TaskListRequest, TaskShowReply, TaskShowRequest, DataManagement};
use swarm::prelude::Dht;
use tonic::{Request, Response, Status};

use crate::image::ImageManager;
use crate::task::{TaskHandle, TaskManager, TaskStatus};
use crate::task::fill::FillTask;
use crate::task::load::{LoadEarthExplorerTask, LoadFormat};
use crate::task::split::SplitTask;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct DataManagementImpl {
    image_manager: Arc<ImageManager>,
    dht: Arc<RwLock<Dht>>,
    task_manager: Arc<RwLock<TaskManager>>,
}

impl DataManagementImpl {
    pub fn new(dht: Arc<RwLock<Dht>>, image_manager: Arc<ImageManager>,
            task_manager: Arc<RwLock<TaskManager>>) -> DataManagementImpl {
        DataManagementImpl {
            dht: dht,
            image_manager: image_manager,
            task_manager: task_manager,
        }
    }
}

#[tonic::async_trait]
impl DataManagement for DataManagementImpl {
    async fn fill(&self, request: Request<FillRequest>)
            -> Result<Response<FillReply>, Status> {
        trace!("FillRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        let task = FillTask::new(request.geohash.clone(),
            self.image_manager.clone(), request.platform.clone(),
            request.thread_count as u8, request.window_seconds);

        // execute task using task manager
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task).unwrap() // TODO - handle error
        };

        // initialize reply
        let reply = FillReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    async fn fill_all(&self, request: Request<FillAllRequest>)
            -> Result<Response<FillAllReply>, Status> {
        trace!("FillAllRequest: {:?}", request);
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

                dht_nodes.push((*node_id, addrs.1.unwrap().clone()));
            }
        }

        // send FillRequest to each dht node
        let mut nodes = HashMap::new();
        for (node_id, addr) in dht_nodes {
            // initialize grpc client
            // TODO - unwrap on await
            let mut client = DataManagementClient::connect(
                format!("http://{}", addr)).await.unwrap();

            // initialize request
            let request = Request::new(FillRequest {
                geohash: request.geohash.clone(),
                platform: request.platform.clone(),
                thread_count: request.thread_count,
                window_seconds: request.window_seconds,
            });

            // retrieve reply
            // TODO - unwrap on await
            let reply = client.fill(request).await.unwrap();
            let reply = reply.get_ref();

            // add images
            nodes.insert(node_id as u32, reply.to_owned());
        }

        // initialize reply
        let reply = FillAllReply {
            nodes: nodes,
        };

        Ok(Response::new(reply))
    }

    async fn load(&self, request: Request<LoadRequest>)
            -> Result<Response<LoadReply>, Status> {
        trace!("LoadDirectoryRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        /*let image_format = match ProtoImageFormat
                ::from_i32(request.image_format).unwrap() {
            ProtoImageFormat::Jpeg => ImageFormat::Jpeg,
            ProtoImageFormat::Tiff => ImageFormat::Tiff,
        };*/

        let load_format = match ProtoLoadFormat
                ::from_i32(request.load_format).unwrap() {
            ProtoLoadFormat::Landsat => LoadFormat::Landsat,
            ProtoLoadFormat::Sentinel => LoadFormat::Sentinel,
        };

        let task = LoadEarthExplorerTask::new(self.dht.clone(),
            request.directory.clone(), request.file.clone(),
            load_format, request.precision as usize,
            request.thread_count as u8);

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

    async fn search(&self, request: Request<SearchRequest>)
            -> Result<Response<SearchReply>, Status> {
        trace!("SearchRequest: {:?}", request);
        let request = request.get_ref();

        // search for the requested images - TODO error
        let images = self.image_manager.search(&request.dataset,
                &request.geohash, &request.platform).unwrap().iter()
            .map(|x| Image {
                coverage: x.coverage,
                dataset: x.dataset.clone(),
                end_date: x.end_date,
                geohash: x.geohash.clone(),
                path: x.path.clone(),
                platform: x.platform.clone(),
                start_date: x.start_date,
            }).collect();

        // initialize reply
        let reply = SearchReply {
            images: images,
        };

        Ok(Response::new(reply))
    }

    async fn search_all(&self, request: Request<SearchAllRequest>)
            -> Result<Response<SearchAllReply>, Status> {
        trace!("SearchAllRequest: {:?}", request);
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

                dht_nodes.push((*node_id, addrs.1.unwrap().clone()));
            }
        }

        // send SearchRequest to each dht node
        let mut nodes = HashMap::new();
        for (node_id, addr) in dht_nodes {
            // initialize grpc client
            // TODO - unwrap on await
            let mut client = DataManagementClient::connect(
                format!("http://{}", addr)).await.unwrap();

            // initialize request
            let request = Request::new(SearchRequest {
                dataset: request.dataset.clone(),
                geohash: request.geohash.clone(),
                platform: request.platform.clone(),
            });

            // retrieve reply
            // TODO - unwrap on await
            let reply = client.search(request).await.unwrap();
            let reply = reply.get_ref();

            // add images
            nodes.insert(node_id as u32, reply.to_owned());
        }

        // initialize reply
        let reply = SearchAllReply {
            nodes: nodes,
        };

        Ok(Response::new(reply))
    }

    async fn split(&self, request: Request<SplitRequest>)
            -> Result<Response<SplitReply>, Status> {
        trace!("SplitRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        let task = SplitTask::new(request.dataset.clone(),
            self.dht.clone(), request.geohash.clone(),
            self.image_manager.clone(), request.platform.clone(),
            request.precision as usize, request.thread_count as u8);

        // execute task using task manager
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task).unwrap() // TODO - handle error
        };

        // initialize reply
        let reply = SplitReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    async fn split_all(&self, request: Request<SplitAllRequest>)
            -> Result<Response<SplitAllReply>, Status> {
        trace!("SplitAllRequest: {:?}", request);
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

                dht_nodes.push((*node_id, addrs.1.unwrap().clone()));
            }
        }

        // send SplitRequest to each dht node
        let mut nodes = HashMap::new();
        for (node_id, addr) in dht_nodes {
            // initialize grpc client
            // TODO - unwrap on await
            let mut client = DataManagementClient::connect(
                format!("http://{}", addr)).await.unwrap();

            // initialize request
            let request = Request::new(SplitRequest {
                dataset: request.dataset.clone(),
                geohash: request.geohash.clone(),
                platform: request.platform.clone(),
                precision: request.precision,
                thread_count: request.thread_count,
            });

            // retrieve reply
            // TODO - unwrap on await
            let reply = client.split(request).await.unwrap();
            let reply = reply.get_ref();

            // add images
            nodes.insert(node_id as u32, reply.to_owned());
        }

        // initialize reply
        let reply = SplitAllReply {
            nodes: nodes,
        };

        Ok(Response::new(reply))
    }

    async fn task_list(&self, request: Request<TaskListRequest>)
            -> Result<Response<TaskListReply>, Status> {
        trace!("TaskListRequest: {:?}", request);

        // populate tasks from task_manager
        let mut tasks = Vec::new();
        {
            let task_manager = self.task_manager.read().unwrap();
            for (task_id, task_handle) in task_manager.iter() {
                // convert TaskHandle to protobuf
                let task = to_protobuf_task(*task_id, task_handle);

                // add to tasks
                tasks.push(task);
            }
        }

        // initialize reply
        let reply = TaskListReply {
            tasks: tasks,
        };

        Ok(Response::new(reply))
    }

    async fn task_list_all(&self, request: Request<TaskListAllRequest>)
            -> Result<Response<TaskListAllReply>, Status> {
        trace!("TaskListAllRequest: {:?}", request);

        // copy valid dht nodes
        let mut dht_nodes = Vec::new();
        {
            let dht = self.dht.read().unwrap();
            for (node_id, addrs) in dht.iter() {
                // check if rpc address is populated
                if let None = addrs.1 {
                    continue;
                }

                dht_nodes.push((*node_id, addrs.1.unwrap().clone()));
            }
        }

        // send SearchRequest to each dht node
        let mut nodes = HashMap::new();
        for (node_id, addr) in dht_nodes {
            // initialize grpc client
            // TODO - unwrap on await
            let mut client = DataManagementClient::connect(
                format!("http://{}", addr)).await.unwrap();

            // initialize request
            let request = Request::new(TaskListRequest {});

            // retrieve reply
            // TODO - unwrap on await
            let reply = client.task_list(request).await.unwrap();
            let reply = reply.get_ref();

            // add images
            nodes.insert(node_id as u32, reply.to_owned());
        }

        // initialize reply
        let reply = TaskListAllReply {
            nodes: nodes,
        };

        Ok(Response::new(reply))
    }

    async fn task_show(&self, request: Request<TaskShowRequest>)
            -> Result<Response<TaskShowReply>, Status> {
        trace!("TaskShowRequest: {:?}", request);
        let request = request.get_ref();

        // populate task from task_manager
        let task = {
            let task_manager = self.task_manager.read().unwrap();
            match task_manager.get(&request.id) {
                None => None,
                Some(task_handle) =>
                    Some(to_protobuf_task(request.id, task_handle)),
            }
        };

        // initialize reply
        let reply = TaskShowReply {
            task: task,
        };

        Ok(Response::new(reply))
    }
}

fn to_protobuf_task(task_id: u64, task_handle: &Arc<RwLock<TaskHandle>>) -> Task {
    // get read lock on TaskHandle
    let task_handle = task_handle.read().unwrap();
    
    // compile task status
    let status = match task_handle.get_status() {
        TaskStatus::Complete => protobuf::TaskStatus::Complete,
        TaskStatus::Failure(_) => protobuf::TaskStatus::Failure,
        TaskStatus::Running => protobuf::TaskStatus::Running,
    };

    // initialize task protobuf
    Task {
        id: task_id,
        completion_percent: task_handle
            .get_completion_percent().unwrap_or(0.0),
        status: status as i32,
    }
}
