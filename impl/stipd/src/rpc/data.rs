use protobuf::{self, DataBroadcastReply, DataBroadcastRequest, DataBroadcastType, DataFillReply, DataFillRequest, DataListRequest, DataManagement, DataManagementClient, DataLoadReply, DataLoadRequest, DataSearchRequest, DataSplitReply, DataSplitRequest, Extent, Image, LoadFormat as ProtoLoadFormat};
use swarm::prelude::Dht;
use tokio::sync::mpsc::Receiver;
use tonic::{Request, Response, Status};

use crate::image::ImageManager;
use crate::task::TaskManager;
use crate::task::fill::FillTask;
use crate::task::load::{LoadEarthExplorerTask, LoadFormat};
use crate::task::split::SplitTask;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct DataManagementImpl {
    image_manager: Arc<RwLock<ImageManager>>,
    dht: Arc<RwLock<Dht>>,
    task_manager: Arc<RwLock<TaskManager>>,
}

impl DataManagementImpl {
    pub fn new(dht: Arc<RwLock<Dht>>,
            image_manager: Arc<RwLock<ImageManager>>,
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
    async fn broadcast(&self, request: Request<DataBroadcastRequest>)
            -> Result<Response<DataBroadcastReply>, Status> {
        trace!("DataBroadcastRequest: {:?}", request);
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
        let mut split_replies = HashMap::new();

        let mut task_id = None;
        for (node_id, addr) in dht_nodes {
            // initialize grpc client - TODO error
            let mut client = DataManagementClient::connect(
                format!("http://{}", addr)).await.unwrap();

            // execute message at dht node
            match DataBroadcastType::from_i32(request.message_type).unwrap() {
                DataBroadcastType::Fill => {
                    // compile new FillRequest
                    let mut fill_request =
                        request.fill_request.clone().unwrap();
                    if let Some(task_id) = task_id {
                        fill_request.task_id = Some(task_id);
                    }

                    // submit request
                    let reply = client.fill(fill_request).await.unwrap();
                    fill_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());

                    // process reply
                    task_id = Some(reply.get_ref().task_id);
                },
                DataBroadcastType::Split => {
                    // compile new SplitRequest
                    let mut split_request =
                        request.split_request.clone().unwrap();
                    if let Some(task_id) = task_id {
                        split_request.task_id = Some(task_id);
                    }

                    // submit request
                    let reply = client.split(split_request).await.unwrap();
                    split_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());

                    // process reply
                    task_id = Some(reply.get_ref().task_id);
                },
            };
        }

        // initialize reply
        let reply = DataBroadcastReply {
            message_type: request.message_type,
            fill_replies: fill_replies,
            split_replies: split_replies,
        };

        Ok(Response::new(reply))
    }

    async fn fill(&self, request: Request<DataFillRequest>)
            -> Result<Response<DataFillReply>, Status> {
        trace!("DataFillRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        let task = FillTask::new(request.band.clone(),
            request.end_timestamp.clone(), request.geohash.clone(),
            self.image_manager.clone(), request.platform.clone(),
            request.recurse, request.start_timestamp.clone(),
            request.thread_count as u8, request.window_seconds);

        // execute task using task manager - TODO error
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task, request.task_id).unwrap()
        };

        // initialize reply
        let reply = DataFillReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    type ListStream = Receiver<Result<Image, Status>>;
    async fn list(&self, request: Request<DataListRequest>)
            -> Result<Response<Self::ListStream>, Status> {
        trace!("DataListRequest: {:?}", request);
        let request = request.get_ref();

        // search for requested images
        let images: Vec<Image> = {
            let image_manager = self.image_manager.read().unwrap();
            image_manager.list(&request.band, &request.end_timestamp,
                &request.geohash, &request.max_cloud_coverage,
                &request.min_pixel_coverage, &request.platform,
                request.recurse, &request.source,
                &request.start_timestamp).iter()
                    .map(|x| Image {
                        band: x.band.clone(),
                        cloud_coverage: x.cloud_coverage,
                        geohash: x.geohash.clone(),
                        path: x.path.clone(),
                        pixel_coverage: x.pixel_coverage,
                        platform: x.platform.clone(),
                        source: x.source.clone(),
                        timestamp: x.timestamp,
                    }).collect()
        };

        // send images though Sender channel
        let (mut tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            for image in images {
                tx.send(Ok(image)).await.unwrap(); // TODO - error
            }
        });

        Ok(Response::new(rx))
    }

    async fn load(&self, request: Request<DataLoadRequest>)
            -> Result<Response<DataLoadReply>, Status> {
        trace!("DataLoadRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        let load_format = match ProtoLoadFormat
                ::from_i32(request.load_format).unwrap() {
            ProtoLoadFormat::Naip => LoadFormat::NAIP,
            ProtoLoadFormat::Sentinel => LoadFormat::Sentinel,
        };

        let task = LoadEarthExplorerTask::new(self.dht.clone(),
            request.directory.clone(), load_format,
            request.precision as usize, request.thread_count as u8);

        // execute task using task manager - TODO error
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task, request.task_id).unwrap()
        };

        // initialize reply
        let reply = DataLoadReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    type SearchStream = Receiver<Result<Extent, Status>>;
    async fn search(&self, request: Request<DataSearchRequest>)
            -> Result<Response<Self::SearchStream>, Status> {
        trace!("DataSearchRequest: {:?}", request);
        let request = request.get_ref();

        // search for requested images
        let extents: Vec<Extent> = {
            let image_manager = self.image_manager.read().unwrap();
            image_manager.search(&request.band, &request.end_timestamp,
                &request.geohash, &request.max_cloud_coverage,
                &request.min_pixel_coverage, &request.platform,
                request.recurse, &request.source,
                &request.start_timestamp).iter()
                    .map(|x| Extent {
                        band: x.band.clone(),
                        count: x.count as u32,
                        geohash: x.geohash.clone(),
                        platform: x.platform.clone(),
                        precision: x.precision as u32,
                        source: x.source.clone(),
                    }).collect()
        };

        // send extents though Sender channel
        let (mut tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            for extent in extents {
                tx.send(Ok(extent)).await.unwrap(); // TODO - error
            }
        });

        Ok(Response::new(rx))
    }

    async fn split(&self, request: Request<DataSplitRequest>)
            -> Result<Response<DataSplitReply>, Status> {
        trace!("SplitRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        let task = SplitTask::new(request.band.clone(), self.dht.clone(),
            request.end_timestamp.clone(), request.geohash.clone(),
            self.image_manager.clone(), request.platform.clone(),
            request.precision as usize, request.recurse,
            request.start_timestamp.clone(), request.thread_count as u8);

        // execute task using task manager - TODO error
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task, request.task_id).unwrap()
        };

        // initialize reply
        let reply = DataSplitReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }
}
