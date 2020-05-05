use protobuf::{self, BroadcastReply, BroadcastRequest, BroadcastType, DataManagementClient, Extent, FillReply, FillRequest, Image, ListRequest, ListReply, LoadFormat as ProtoLoadFormat, LoadReply, LoadRequest, SearchReply, SearchRequest, SplitReply, SplitRequest, Task, TaskListReply, TaskListRequest, TaskShowReply, TaskShowRequest, DataManagement};
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
    async fn broadcast(&self, request: Request<BroadcastRequest>)
            -> Result<Response<BroadcastReply>, Status> {
        trace!("BroadcastRequest: {:?}", request);
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

        // send broadcast message to each dht node
        let mut fill_replies = HashMap::new();
        let mut list_replies = HashMap::new();
        let mut search_replies = HashMap::new();
        let mut split_replies = HashMap::new();
        let mut task_list_replies = HashMap::new();

        for (node_id, addr) in dht_nodes {
            // initialize grpc client - TODO error
            let mut client = DataManagementClient::connect(
                format!("http://{}", addr)).await.unwrap();

            // execute message at dht node
            match BroadcastType::from_i32(request.message_type).unwrap() {
                BroadcastType::Fill => {
                    let reply = client.fill(request
                        .fill_request.clone().unwrap()).await.unwrap();
                    fill_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                BroadcastType::List => {
                    let reply = client.list(request
                        .list_request.clone().unwrap()).await.unwrap();
                    list_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                BroadcastType::Search => {
                    let reply = client.search(request
                        .search_request.clone().unwrap()).await.unwrap();
                    search_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                BroadcastType::Split => {
                    let reply = client.split(request
                        .split_request.clone().unwrap()).await.unwrap();
                    split_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                BroadcastType::TaskList => {
                    let reply = client.task_list(request
                        .task_list_request.clone().unwrap()).await.unwrap();
                    task_list_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
            };
        }

        // initialize reply
        let reply = BroadcastReply {
            message_type: request.message_type,
            fill_replies: fill_replies,
            list_replies: list_replies,
            search_replies: search_replies,
            split_replies: split_replies,
            task_list_replies: task_list_replies,
        };

        Ok(Response::new(reply))
    }

    async fn fill(&self, request: Request<FillRequest>)
            -> Result<Response<FillReply>, Status> {
        trace!("FillRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        let task = FillTask::new(request.band.clone(),
            request.geohash.clone(), self.image_manager.clone(),
            request.platform.clone(), request.thread_count as u8,
            request.window_seconds);

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

    async fn list(&self, request: Request<ListRequest>)
            -> Result<Response<ListReply>, Status> {
        trace!("ListRequest: {:?}", request);
        let request = request.get_ref();

        // search for the requested images - TODO error
        let images = self.image_manager.search(&request.band,
                &request.dataset, &request.geohash,
                &request.platform, false).unwrap().iter()
            .map(|x| Image {
                band: x.band.clone(),
                cloud_coverage: x.cloud_coverage,
                dataset: x.dataset.clone(),
                end_date: x.end_date,
                geohash: x.geohash.clone(),
                path: x.path.clone(),
                pixel_coverage: x.pixel_coverage,
                platform: x.platform.clone(),
                start_date: x.start_date,
            }).collect();

        // initialize reply
        let reply = ListReply {
            images: images,
        };

        Ok(Response::new(reply))
    }

    async fn load(&self, request: Request<LoadRequest>)
            -> Result<Response<LoadReply>, Status> {
        trace!("LoadDirectoryRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        let load_format = match ProtoLoadFormat
                ::from_i32(request.load_format).unwrap() {
            ProtoLoadFormat::Landsat => LoadFormat::Landsat,
            ProtoLoadFormat::Sentinel => LoadFormat::Sentinel,
        };

        let task = LoadEarthExplorerTask::new(self.dht.clone(),
            request.directory.clone(), load_format,
            request.precision as usize, request.thread_count as u8);

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
        let images = self.image_manager.search(&request.band,
                &request.dataset, &request.geohash,
                &request.platform, true).unwrap();

        // compile extents
        let mut platform_map = HashMap::new();
        let precision = request.geohash.len() + 1;
        for image in images {
            let geohash_map = platform_map.entry(
                image.platform.clone()).or_insert(HashMap::new());

            let geohash = image.geohash[..std::cmp::min(precision, image.geohash.len())].to_string();
            let band_map = geohash_map.entry(geohash)
                .or_insert(HashMap::new());

            let dataset_map = band_map.entry(image.band.clone())
                .or_insert(HashMap::new());

            let count_map = dataset_map.entry(
                image.dataset.clone()).or_insert(HashMap::new());

            let count = count_map.entry(image.geohash.len())
                .or_insert(0);
            *count += 1;
        }

        // convert to protobuf format
        let mut extents = Vec::new();
        for (platform, geohash_map) in platform_map.iter() {
            for (geohash, band_map) in geohash_map.iter() {
                for (band, dataset_map) in band_map.iter() {
                    for (dataset, count_map) in dataset_map.iter() {
                        for (precision, count) in count_map.iter() {
                            extents.push(Extent {
                                band: band.clone(),
                                count: *count,
                                dataset: dataset.clone(),
                                geohash: geohash.clone(),
                                platform: platform.clone(),
                                precision: *precision as u32,
                            });
                        }
                    }
                }
            }
        }

        // initialize reply
        let reply = SearchReply {
            extents: extents,
        };

        Ok(Response::new(reply))
    }

    async fn split(&self, request: Request<SplitRequest>)
            -> Result<Response<SplitReply>, Status> {
        trace!("SplitRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        let task = SplitTask::new(request.band.clone(),
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
            .get_completion_percent().unwrap_or(1.0),
        status: status as i32,
    }
}
