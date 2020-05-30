use protobuf::{self, DataBroadcastReply, DataBroadcastRequest, DataBroadcastType, DataFillReply, DataFillRequest, DataListRequest, DataManagement, DataManagementClient, DataLoadReply, DataLoadRequest, DataSearchRequest, DataSplitReply, DataSplitRequest, Extent, File, Image, LoadFormat as ProtoLoadFormat};
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
        /*let task = FillTask::new(
            request.end_timestamp.clone(), request.geohash.clone(),
            self.image_manager.clone(), request.platform.clone(),
            request.recurse, request.start_timestamp.clone(),
            request.thread_count as u8, request.window_seconds);

        // execute task using task manager - TODO error
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task, request.task_id).unwrap()
        };*/
        let task_id = 0; // TODO - fix fill

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
        let filter = &request.filter;

        // search for requested images
        let mut images = Vec::new();
        {
            let image_manager = self.image_manager.read().unwrap();
            let image_iter = image_manager.list(&filter.end_timestamp,
                &filter.geohash, &filter.max_cloud_coverage,
                &filter.min_pixel_coverage, &filter.platform,
                filter.recurse, &filter.source, &filter.start_timestamp);

            // convert image and files to protobufs
            for (i, f) in image_iter {
                let mut files = Vec::new();
                for file in f {
                    files.push(File {
                        description: file.0,
                        path: file.1,
                        pixel_coverage: file.2,
                    })
                }

                images.push(Image {
                    cloud_coverage: i.0,
                    geohash: i.1,
                    files: files,
                    platform: i.2,
                    source: i.3,
                    timestamp: i.5,
                });
            }
        }

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
            ProtoLoadFormat::Modis => LoadFormat::MODIS,
            ProtoLoadFormat::Naip => LoadFormat::NAIP,
            ProtoLoadFormat::Sentinel => LoadFormat::Sentinel,
        };

        let task = LoadEarthExplorerTask::new(self.dht.clone(),
            request.glob.clone(), load_format,
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
        let filter = &request.filter;

        // search for requested images
        let extents: Vec<Extent> = {
            let image_manager = self.image_manager.read().unwrap();
            image_manager.search(&filter.end_timestamp, &filter.geohash,
                &filter.max_cloud_coverage, &filter.min_pixel_coverage,
                &filter.platform, filter.recurse, &filter.source,
                &filter.start_timestamp).iter()
                    .map(|x| Extent {
                        count: x.0 as u32,
                        geohash: x.1.clone(),
                        platform: x.2.clone(),
                        precision: x.3 as u32,
                        source: x.4.clone(),
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
        let filter = &request.filter;

        // initialize task
        let task = SplitTask::new(self.dht.clone(),
            filter.end_timestamp.clone(), filter.geohash.clone(),
            self.image_manager.clone(), filter.platform.clone(),
            request.precision as usize, filter.recurse,
            filter.start_timestamp.clone(), request.thread_count as u8);

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
