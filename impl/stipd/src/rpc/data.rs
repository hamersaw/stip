use protobuf::{self, DataBroadcastReply, DataBroadcastRequest, DataBroadcastType, DataFillReply, DataFillRequest, DataListRequest, DataListReply, DataManagement, DataManagementClient, DataLoadReply, DataLoadRequest, DataSearchReply, DataSearchRequest, DataSplitReply, DataSplitRequest, Extent, Image, LoadFormat as ProtoLoadFormat};
use swarm::prelude::Dht;
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
        let mut list_replies = HashMap::new();
        let mut search_replies = HashMap::new();
        let mut split_replies = HashMap::new();

        for (node_id, addr) in dht_nodes {
            // initialize grpc client - TODO error
            let mut client = DataManagementClient::connect(
                format!("http://{}", addr)).await.unwrap();

            // execute message at dht node
            match DataBroadcastType::from_i32(request.message_type).unwrap() {
                DataBroadcastType::Fill => {
                    let reply = client.fill(request
                        .fill_request.clone().unwrap()).await.unwrap();
                    fill_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                DataBroadcastType::List => {
                    let reply = client.list(request
                        .list_request.clone().unwrap()).await.unwrap();
                    list_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                DataBroadcastType::Search => {
                    let reply = client.search(request
                        .search_request.clone().unwrap()).await.unwrap();
                    search_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
                DataBroadcastType::Split => {
                    let reply = client.split(request
                        .split_request.clone().unwrap()).await.unwrap();
                    split_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());
                },
            };
        }

        // initialize reply
        let reply = DataBroadcastReply {
            message_type: request.message_type,
            fill_replies: fill_replies,
            list_replies: list_replies,
            search_replies: search_replies,
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
            request.geohash.clone(), self.image_manager.clone(),
            request.platform.clone(), request.thread_count as u8,
            request.window_seconds);

        // execute task using task manager
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task).unwrap() // TODO - handle error
        };

        // initialize reply
        let reply = DataFillReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    async fn list(&self, request: Request<DataListRequest>)
            -> Result<Response<DataListReply>, Status> {
        trace!("DataListRequest: {:?}", request);
        let request = request.get_ref();

        // search for the requested images
        let images = {
            let image_manager = self.image_manager.read().unwrap();
            image_manager.search(&request.band,
                    &request.geohash, &request.platform,
                    false, &request.source).iter()
                .map(|x| Image {
                    band: x.band.clone(),
                    cloud_coverage: x.cloud_coverage,
                    end_date: x.end_date,
                    geohash: x.geohash.clone(),
                    path: x.path.clone(),
                    pixel_coverage: x.pixel_coverage,
                    platform: x.platform.clone(),
                    source: x.source.clone(),
                    start_date: x.start_date,
                }).collect()
        };

        // initialize reply
        let reply = DataListReply {
            images: images,
        };

        Ok(Response::new(reply))
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

        // execute task using task manager
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            task_manager.execute(task).unwrap() // TODO - handle error
        };

        // initialize reply
        let reply = DataLoadReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    async fn search(&self, request: Request<DataSearchRequest>)
            -> Result<Response<DataSearchReply>, Status> {
        trace!("DataSearchRequest: {:?}", request);
        let request = request.get_ref();

        // search for the requested images - TODO error
        let image_manager = self.image_manager.read().unwrap();
        let images = image_manager.search(&request.band,
            &request.geohash, &request.platform, true, &request.source);

        // compile extents
        let mut platform_map = HashMap::new();
        let precision = match &request.geohash {
            Some(geohash) => geohash.len() + 1,
            None => 1,
        };

        for image in images {
            let geohash_map = platform_map.entry(
                image.platform.clone()).or_insert(HashMap::new());

            let geohash = image.geohash[..std::cmp::min(precision, image.geohash.len())].to_string();
            let band_map = geohash_map.entry(geohash)
                .or_insert(HashMap::new());

            let source_map = band_map.entry(image.band.clone())
                .or_insert(HashMap::new());

            let count_map = source_map.entry(
                image.source.clone()).or_insert(HashMap::new());

            let count = count_map.entry(image.geohash.len())
                .or_insert(0);
            *count += 1;
        }

        // convert to protobuf format
        let mut extents = Vec::new();
        for (platform, geohash_map) in platform_map.iter() {
            for (geohash, band_map) in geohash_map.iter() {
                for (band, source_map) in band_map.iter() {
                    for (source, count_map) in source_map.iter() {
                        for (precision, count) in count_map.iter() {
                            extents.push(Extent {
                                band: band.clone(),
                                count: *count,
                                geohash: geohash.clone(),
                                platform: platform.clone(),
                                precision: *precision as u32,
                                source: source.clone(),
                            });
                        }
                    }
                }
            }
        }

        // initialize reply
        let reply = DataSearchReply {
            extents: extents,
        };

        Ok(Response::new(reply))
    }

    async fn split(&self, request: Request<DataSplitRequest>)
            -> Result<Response<DataSplitReply>, Status> {
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
        let reply = DataSplitReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }
}
