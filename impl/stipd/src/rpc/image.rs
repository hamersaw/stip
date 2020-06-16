use protobuf::{self, ImageBroadcastReply, ImageBroadcastRequest, ImageBroadcastType, ImageCoalesceReply, ImageCoalesceRequest, ImageFillReply, ImageFillRequest, ImageListRequest, ImageManagement, ImageManagementClient, ImageStoreReply, ImageStoreRequest, ImageSearchRequest, ImageSplitReply, ImageSplitRequest, Extent, File, Image, ImageFormat as ProtoImageFormat};
use swarm::prelude::Dht;
use tokio::sync::mpsc::Receiver;
use tonic::{Code, Request, Response, Status};

use crate::album::AlbumManager;
use crate::task::{Task, TaskOg, TaskManager};
use crate::task::coalesce::CoalesceTask;
//use crate::task::fill::FillTask;
use crate::task::store::{StoreEarthExplorerTask, ImageFormat};
use crate::task::split::SplitTask;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct ImageManagementImpl {
    album_manager: Arc<RwLock<AlbumManager>>,
    dht: Arc<RwLock<Dht>>,
    task_manager: Arc<RwLock<TaskManager>>,
}

impl ImageManagementImpl {
    pub fn new(album_manager: Arc<RwLock<AlbumManager>>,
            dht: Arc<RwLock<Dht>>,
            task_manager: Arc<RwLock<TaskManager>>) -> ImageManagementImpl {
        ImageManagementImpl {
            album_manager: album_manager,
            dht: dht,
            task_manager: task_manager,
        }
    }
}

#[tonic::async_trait]
impl ImageManagement for ImageManagementImpl {
    async fn broadcast(&self, request: Request<ImageBroadcastRequest>)
            -> Result<Response<ImageBroadcastReply>, Status> {
        trace!("ImageBroadcastRequest: {:?}", request);
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
        let mut coalesce_replies = HashMap::new();
        let mut fill_replies = HashMap::new();
        let mut split_replies = HashMap::new();

        let mut task_id = None;
        for (node_id, addr) in dht_nodes {
            // initialize grpc client
            let mut client = match ImageManagementClient::connect(
                    format!("http://{}", addr)).await {
                Ok(client) => client,
                Err(e) => return Err(Status::new(Code::Unavailable,
                    format!("connection to {} failed: {}", addr, e))),
            };

            // execute message at dht node
            match ImageBroadcastType::from_i32(request.message_type).unwrap() {
                ImageBroadcastType::Coalesce => {
                    // compile new CoalesceRequest
                    let mut coalesce_request =
                        request.coalesce_request.clone().unwrap();
                    if let Some(task_id) = task_id {
                        coalesce_request.task_id = Some(task_id);
                    }

                    // submit request
                    let reply = match client.coalesce(coalesce_request).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("coalesce broadcast failed: {}", e))),
                    };
                    coalesce_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());

                    // process reply
                    task_id = Some(reply.get_ref().task_id);
                },
                ImageBroadcastType::Fill => {
                    // compile new FillRequest
                    let mut fill_request =
                        request.fill_request.clone().unwrap();
                    if let Some(task_id) = task_id {
                        fill_request.task_id = Some(task_id);
                    }

                    // submit request
                    let reply = match client.fill(fill_request).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("fill broadcast failed: {}", e))),
                    };
                    fill_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());

                    // process reply
                    task_id = Some(reply.get_ref().task_id);
                },
                ImageBroadcastType::Split => {
                    // compile new SplitRequest
                    let mut split_request =
                        request.split_request.clone().unwrap();
                    if let Some(task_id) = task_id {
                        split_request.task_id = Some(task_id);
                    }

                    // submit request
                    let reply = match client.split(split_request).await {
                        Ok(reply) => reply,
                        Err(e) => return Err(Status::new(Code::Unknown,
                            format!("split broadcast failed: {}", e))),
                    };
                    split_replies.insert(node_id as u32,
                        reply.get_ref().to_owned());

                    // process reply
                    task_id = Some(reply.get_ref().task_id);
                },
            };
        }

        // initialize reply
        let reply = ImageBroadcastReply {
            message_type: request.message_type,
            coalesce_replies: coalesce_replies,
            fill_replies: fill_replies,
            split_replies: split_replies,
        };

        Ok(Response::new(reply))
    }

    async fn coalesce(&self, request: Request<ImageCoalesceRequest>)
            -> Result<Response<ImageCoalesceReply>, Status> {
        trace!("ImageCoalesceRequest: {:?}", request);
        let request = request.get_ref();
        let filter = &request.filter;

        // ensure album exists
        let album = crate::rpc::assert_album_exists(
            &self.album_manager, &request.album)?;

        // initailize task
        let task = CoalesceTask::new(album, self.dht.clone(),
            filter.end_timestamp, filter.geocode.clone(),
            filter.max_cloud_coverage, filter.min_pixel_coverage,
            filter.platform.clone(), filter.recurse, filter.source.clone(),
            request.platform.clone(), filter.start_timestamp,
            request.thread_count as u8, request.window_seconds);

        // start task
        /*let task_handle = match task.start().await {
            Ok(task_handle) => task_handle,
            Err(e) => return Err(Status::new(Code::Unknown,
                format!("failed to start CoalesceTask: {}", e))),
        };

        // register task with TaskHandler
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            match task_manager.register(task_handle, request.task_id) {
                Ok(task_id) => task_id,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to register CoalesceTask: {}", e))),
            }
        };*/
        // TODO - test this functionality
        let task_handle = {
            let task = Arc::new(task);
            match task.start(request.thread_count as u8) {
                Ok(task_handle) => task_handle,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to start CoalesceTask: {}", e))),
            }
        };
        let task_id = 0;

        // initialize reply
        let reply = ImageCoalesceReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    async fn fill(&self, request: Request<ImageFillRequest>)
            -> Result<Response<ImageFillReply>, Status> {
        trace!("ImageFillRequest: {:?}", request);
        let request = request.get_ref();

        // initialize task
        /*let task = FillTask::new(
            request.end_timestamp.clone(), request.geocode.clone(),
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
        let reply = ImageFillReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    type ListStream = Receiver<Result<Image, Status>>;
    async fn list(&self, request: Request<ImageListRequest>)
            -> Result<Response<Self::ListStream>, Status> {
        trace!("ImageListRequest: {:?}", request);
        let request = request.get_ref();
        let filter = &request.filter;

        // ensure album exists
        let album = crate::rpc::assert_album_exists(
            &self.album_manager, &request.album)?;

        // search for requested images
        let mut images = Vec::new();
        {
            let album = album.read().unwrap();
            let image_iter = match album.list(&filter.end_timestamp,
                    &filter.geocode, &filter.max_cloud_coverage,
                    &filter.min_pixel_coverage, &filter.platform,
                    filter.recurse, &filter.source,
                    &filter.start_timestamp) {
                Ok(image_iter) => image_iter,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to list images: {}", e))),
            };

            // convert image and files to protobufs
            for (i, f) in image_iter {
                let mut files = Vec::new();
                for file in f {
                    files.push(File {
                        path: file.0,
                        pixel_coverage: file.1,
                        subdataset: file.2 as i32,
                    })
                }

                images.push(Image {
                    cloud_coverage: i.0,
                    geocode: i.1,
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
                if let Err(e) = tx.send(Ok(image)).await {
                    warn!("failed to send image list: {}", e);
                    break;
                }
            }
        });

        Ok(Response::new(rx))
    }

    type SearchStream = Receiver<Result<Extent, Status>>;
    async fn search(&self, request: Request<ImageSearchRequest>)
            -> Result<Response<Self::SearchStream>, Status> {
        trace!("ImageSearchRequest: {:?}", request);
        let request = request.get_ref();
        let filter = &request.filter;

        // ensure album exists
        let album = crate::rpc::assert_album_exists(
            &self.album_manager, &request.album)?;

        // search for requested images
        let extents: Vec<Extent> = {
            let album = album.read().unwrap();
            let extent_iter = match album.search(&filter.end_timestamp,
                    &filter.geocode, &filter.max_cloud_coverage,
                    &filter.min_pixel_coverage, &filter.platform,
                    filter.recurse, &filter.source,
                    &filter.start_timestamp) {
                Ok(extent_iter) => extent_iter,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to search images: {}", e))),
            };
            
            extent_iter.iter().map(|x| Extent {
                count: x.0 as u32,
                geocode: x.1.clone(),
                platform: x.2.clone(),
                precision: x.3 as u32,
                source: x.4.clone(),
            }).collect()
        };

        // send extents though Sender channel
        let (mut tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            for extent in extents {
                if let Err(e) = tx.send(Ok(extent)).await {
                    warn!("failed to send extent list: {}", e);
                    break;
                }
            }
        });

        Ok(Response::new(rx))
    }

    async fn split(&self, request: Request<ImageSplitRequest>)
            -> Result<Response<ImageSplitReply>, Status> {
        trace!("ImageSplitRequest: {:?}", request);
        let request = request.get_ref();
        let filter = &request.filter;

        // ensure album exists
        let album = crate::rpc::assert_album_exists(
            &self.album_manager, &request.album)?;

        // initialize task
        let task = SplitTask::new(album, self.dht.clone(),
            filter.end_timestamp.clone(), filter.geocode.clone(),
            request.geocode_bound.clone(), filter.platform.clone(),
            request.precision as usize, filter.recurse,
            filter.start_timestamp.clone(), request.thread_count as u8);

        // start task
        /*let task_handle = match task.start().await {
            Ok(task_handle) => task_handle,
            Err(e) => return Err(Status::new(Code::Unknown,
                format!("failed to start SplitTask: {}", e))),
        };

        // register task with TaskHandler
        let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            match task_manager.register(task_handle, request.task_id) {
                Ok(task_id) => task_id,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to register SplitTask: {}", e))),
            }
        };*/
        // TODO - test this functionality
        let task_handle = {
            let task = Arc::new(task);
            match task.start(request.thread_count as u8) {
                Ok(task_handle) => task_handle,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to start SplitTask: {}", e))),
            }
        };
        let task_id = 0;
 
        // initialize reply
        let reply = ImageSplitReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }

    async fn store(&self, request: Request<ImageStoreRequest>)
            -> Result<Response<ImageStoreReply>, Status> {
        trace!("ImageStoreRequest: {:?}", request);
        let request = request.get_ref();
 
        // ensure album exists
        let album = crate::rpc::assert_album_exists(
            &self.album_manager, &request.album)?;

        // initialize task
        let format = match ProtoImageFormat
                ::from_i32(request.format).unwrap() {
            ProtoImageFormat::Modis => ImageFormat::MODIS,
            ProtoImageFormat::Naip => ImageFormat::NAIP,
            ProtoImageFormat::Sentinel => ImageFormat::Sentinel,
        };

        let task = StoreEarthExplorerTask::new(album, self.dht.clone(),
            format, request.glob.clone(), request.precision as usize,
            request.thread_count as u8);

        // start task
        /*let task_handle = match task.start().await {
            Ok(task_handle) => task_handle,
            Err(e) => return Err(Status::new(Code::Unknown,
                format!("failed to start StoreTask: {}", e))),
        };*/
 
        // TODO - test this functionality
        let task_handle = {
            let task = Arc::new(task);
            match task.start(request.thread_count as u8) {
                Ok(task_handle) => task_handle,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to start OpenTask: {}", e))),
            }
        };

        // register task with TaskHandler
        /*let task_id = {
            let mut task_manager = self.task_manager.write().unwrap();
            match task_manager.register(task_handle, request.task_id) {
                Ok(task_id) => task_id,
                Err(e) => return Err(Status::new(Code::Unknown,
                    format!("failed to register StoreTask: {}", e))),
            }
        };*/
        let task_id = 0;

        // initialize reply
        let reply = ImageStoreReply {
            task_id: task_id,
        };

        Ok(Response::new(reply))
    }
}
