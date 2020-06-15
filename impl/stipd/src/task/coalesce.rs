use failure::ResultExt;
use gdal::raster::Dataset;
use protobuf::{ImageListRequest, Filter, ImageManagementClient};
use swarm::prelude::Dht;
use tonic::Request;

use crate::{Image, StFile, RAW_SOURCE, SPLIT_SOURCE};
use crate::album::Album;
use crate::task::{Task, TaskHandle, TaskStatus};

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct CoalesceTask {
    album: Arc<RwLock<Album>>,
    dht: Arc<RwLock<Dht>>,
    end_timestamp: Option<i64>,
    geocode: Option<String>,
    max_cloud_coverage: Option<f64>,
    min_pixel_coverage: Option<f64>,
    platform: Option<String>,
    source: Option<String>,
    src_platform: String,
    recurse: bool,
    start_timestamp: Option<i64>,
    thread_count: u8,
    window_seconds: i64,
}

impl CoalesceTask {
    pub fn new(album: Arc<RwLock<Album>>, dht: Arc<RwLock<Dht>>,
            end_timestamp: Option<i64>, geocode: Option<String>,
            max_cloud_coverage: Option<f64>,
            min_pixel_coverage: Option<f64>, platform: Option<String>,
            recurse: bool, source: Option<String>, src_platform: String,
            start_timestamp: Option<i64>, thread_count: u8,
            window_seconds: i64) -> CoalesceTask {
        CoalesceTask {
            album: album,
            dht: dht,
            end_timestamp: end_timestamp,
            geocode: geocode,
            max_cloud_coverage: max_cloud_coverage,
            min_pixel_coverage: min_pixel_coverage,
            platform: platform,
            recurse: recurse,
            source: source,
            src_platform: src_platform,
            start_timestamp: start_timestamp,
            thread_count: thread_count,
            window_seconds: window_seconds,
        }
    }
}

#[tonic::async_trait]
impl Task for CoalesceTask {
    async fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // retrieve album metadata
        let album_id = {
            let album = self.album.read().unwrap();
            album.get_id().to_string()
        };

        // search for source images using Album
        let src_records: Vec<(Image, Vec<StFile>)> = {
            let album = self.album.read().unwrap();
            album.list(&self.end_timestamp, &self.geocode, &None, &None,
                &Some(self.src_platform.clone()), self.recurse,  
                &Some(RAW_SOURCE.to_string()), &self.start_timestamp)?
        };
 
        let mut split_records = HashMap::new();

        // initialize Filter
        let filter = Filter {
            end_timestamp: self.end_timestamp,
            geocode: self.geocode.clone(),
            max_cloud_coverage: self.max_cloud_coverage,
            min_pixel_coverage: self.min_pixel_coverage,
            platform: self.platform.clone(),
            recurse: self.recurse,
            source: self.source.clone(),
            start_timestamp: self.start_timestamp,
        };

        // initialize ImageListRequest
        let request = ImageListRequest {
            album: album_id,
            filter: filter,
        };

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

        // iterate over dht nodes
        for (_, addr) in dht_nodes {
            // open ImageManagementClient
            let mut client = match ImageManagementClient::connect(
                    format!("http://{}", addr)).await {
                Ok(client) => client,
                Err(e) => return Err(format!(
                    "connection to {} failed: {}", addr, e).into()),
            };

            // send ListImagesRequest
            let mut stream = client.list(Request::new(request.clone()))
                .await?.into_inner();

            // iterate over image stream
            let mut src_index = 0;
            let mut message = stream.message().await?;

            loop {
                // if we have exhausted one list -> break
                if src_index >= src_records.len() || message.is_none() {
                    break;
                }

                // compare the current record pair
                let src_record = &src_records[src_index].0;
                let dst_record = message.as_ref().unwrap();

                if (src_record.5 - dst_record.timestamp).abs() 
                        <= self.window_seconds {
                    if src_record.1 == dst_record.geocode {
                        // geocodes are equal -> increment lowest timestamp
                        if src_record.5 < dst_record.timestamp {
                            src_index += 1;
                        } else {
                            message = stream.message().await?;
                        }
                    } else if dst_record.geocode
                            .starts_with(&src_record.1) {
                        // append pair to split_records
                        let geocodes = split_records.entry(src_index)
                            .or_insert(HashSet::new());
                        geocodes.insert(dst_record.geocode.clone());

                        message = stream.message().await?;
                    } else if src_record.1
                            .starts_with(&dst_record.geocode) {
                        // TODO - merge
                        unimplemented!();
                    } else {
                        // increment lowest geohash
                        if src_record.1 < dst_record.geocode {
                            src_index += 1;
                        } else {
                            message = stream.message().await?;
                        }
                    }
                } else if src_record.5 < dst_record.timestamp {
                    src_index += 1;
                } else {
                    message = stream.message().await?;
                }
            }
        }

        // initialize record channel
        let (sender, receiver) = crossbeam_channel::bounded(256);

        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let items_skipped = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let album = self.album.clone();
            let dht = self.dht.clone();
            let items_completed = items_completed.clone();
            let items_skipped = items_skipped.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                // iterate over records
                loop {
                    // fetch next record
                    let record: (Image, Vec<StFile>, HashSet<String>) = 
                            match receiver_clone.recv() {
                        Ok(record) => record,
                        Err(_) => break,
                    };

                    // process record
                    match process(&album, &dht, &record) {
                        Ok(_) => items_completed.fetch_add(1, Ordering::SeqCst),
                        Err(e) => {
                            warn!("skipping record '{:?}': {}",
                                &record, e);
                            items_skipped.fetch_add(1, Ordering::SeqCst)
                        },
                    };
                }
            });

            join_handles.push(join_handle);
        }

        // initialize TaskHandle
        let task_handle = Arc::new( RwLock::new(
            TaskHandle::new(
                items_completed,
                items_skipped,
                split_records.len() as u32,
                TaskStatus::Running
            )));

        // start management thread
        let task_handle_clone = task_handle.clone();
        let _ = std::thread::spawn(move || {
            // add items to pipeline
            for (src_index, geocodes) in split_records {
                // TODO - fix clone?
                let record = src_records[src_index].clone();
                if let Err(e) = sender.send(
                        (record.0, record.1, geocodes)) {
                    // set TaskHandle status to 'failed'
                    let mut task_handle =
                        task_handle_clone.write().unwrap();
                    task_handle.set_status(
                        TaskStatus::Failure(format!("{:?}", e)));

                    return;
                }
            }
 
            // drop sender to signal worker threads
            drop(sender);

            // join worker threads
            for join_handle in join_handles {
                if let Err(e) = join_handle.join() {
                    // set TaskHandle status to 'failed'
                    let mut task_handle =
                        task_handle_clone.write().unwrap();
                    task_handle.set_status(
                        TaskStatus::Failure(format!("{:?}", e)));

                    return;
                }
            }

            // set TaskHandle status to 'completed'
            let mut task_handle = task_handle_clone.write().unwrap();
            task_handle.set_status(TaskStatus::Complete);
        });

        // return task handle
        Ok(task_handle)
    }
}

fn process(album: &Arc<RwLock<Album>>, dht: &Arc<RwLock<Dht>>,
        record: &(Image, Vec<StFile>, HashSet<String>))
        -> Result<(), Box<dyn Error>> {
    let image = &record.0;
    let split_geocodes = &record.2;

    // retrieve album metadata
    let (album_id, dht_key_length, geocode) = {
        let album = album.read().unwrap();
        (album.get_id().to_string(), album.get_dht_key_length(),
            album.get_geocode().clone())
    };

    // iterate over split precisions
    let precisions: HashSet<usize> =
        record.2.iter().map(|x| x.len()).collect();

    for precision in precisions {
        for file in record.1.iter() {
            // check if path exists
            let path = {
                let album = album.read().unwrap();
                Path::new(&file.0);
                album.get_image_path(false, &image.1,
                    &image.2, &image.3, file.2, &image.4)?
            };

            if !path.exists() {
                return Err(format!("image path '{}' does not exist",
                    path.to_string_lossy()).into());
            }

            // open image
            let dataset = Dataset::open(&path).compat()?;

            // split image with geocode precision
            for dataset_split in st_image::prelude::split(&dataset,
                    geocode, precision).compat()? {
                // calculate split dataset geocode
                let (win_min_x, win_max_x, win_min_y, win_max_y) =
                    dataset_split.coordinates();
                let split_geocode = geocode.get_code(
                    (win_min_x + win_max_x) / 2.0,
                    (win_min_y + win_max_y) / 2.0, precision)?;

                // skip if geocode is not in split geocodes
                if !split_geocodes.contains(&split_geocode) {
                    continue; 
                }

                // perform dataset split
                let dataset = dataset_split.dataset().compat()?;

                // if image has 0.0 coverage -> don't process
                let pixel_coverage = st_image::coverage(&dataset).compat()?;
                if pixel_coverage == 0f64 {
                    continue;
                }

                // lookup geocode in dht
                let addr = match crate::task::dht_lookup(
                        &dht, dht_key_length, &split_geocode) {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!("{}", e);
                        continue;
                    },
                };

                // send image to new host
                if let Err(e) = crate::transfer::send_image(&addr,
                        &album_id, &dataset, &split_geocode, file.1,
                        &image.2, SPLIT_SOURCE, file.2, &image.4, image.5) {
                    warn!("failed to write image to node {}: {}", addr, e);
                }
            }
        }
    }

    Ok(())
}
