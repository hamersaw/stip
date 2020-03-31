use csv::Reader;
use crossbeam_channel::Receiver;
use image::ImageFormat;
use image::io::Reader as ImageReader;
use serde::Deserialize;
use st_image::StImage;
use swarm::prelude::Dht;

use crate::task::{Task, TaskHandle, TaskStatus};

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hasher;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct LoadEarthExplorerTask {
    dht: Arc<RwLock<Dht>>,
    directory: String,
    file: String,
    precision: usize,
    thread_count: u8,
}

impl LoadEarthExplorerTask {
    pub fn new(dht: Arc<RwLock<Dht>>, directory: String, file: String,
            precision: usize, thread_count: u8) -> LoadEarthExplorerTask {
        LoadEarthExplorerTask {
            dht: dht,
            directory: directory,
            file: file,
            precision: precision,
            thread_count: thread_count,
        }
    }
}

impl Task for LoadEarthExplorerTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // read file records
        let path = Path::new(&self.file);
        let mut reader = Reader::from_path(path)?;

        let mut records = Vec::new();
        for result in reader.deserialize() {
            let record: Record = result?;

            records.push(record);
        }

        // initialize record channel
        let (sender, receiver) = crossbeam_channel::bounded(256);

        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let dht_clone = self.dht.clone();
            let directory_clone = self.directory.clone();
            let items_completed = items_completed.clone();
            let precision_clone = self.precision.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                if let Err(e) = worker_thread(dht_clone,
                        directory_clone, items_completed,
                        precision_clone, receiver_clone) {
                    panic!("worker thread failure: {}", e);
                }
            });

            join_handles.push(join_handle);
        }

        // initialize TaskHandle
        let task_handle = Arc::new( RwLock::new(
            TaskHandle::new(
                items_completed,
                records.len() as u32,
                TaskStatus::Running
            )));

        // start management thread
        let task_handle_clone = task_handle.clone();
        let _ = std::thread::spawn(move || {
            // add items to pipeline
            for record in records {
                if let Err(e) = sender.send(record) {
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

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename(deserialize = "Landsat Product Identifier"))]
    product_id: String,
    #[serde(rename(deserialize = "Spacecraft Identifier"))]
    spacecraft_id: String,
    #[serde(rename(deserialize = "LL Corner Lat dec"))]
    ll_lat: f64,
    #[serde(rename(deserialize = "LL Corner Long dec"))]
    ll_long: f64,
    #[serde(rename(deserialize = "UL Corner Lat dec"))]
    ul_lat: f64,
    #[serde(rename(deserialize = "UL Corner Long dec"))]
    ul_long: f64,
    #[serde(rename(deserialize = "LR Corner Lat dec"))]
    lr_lat: f64,
    #[serde(rename(deserialize = "LR Corner Long dec"))]
    lr_long: f64,
    #[serde(rename(deserialize = "UR Corner Lat dec"))]
    ur_lat: f64,
    #[serde(rename(deserialize = "UR Corner Long dec"))]
    ur_long: f64,
}

fn worker_thread(dht: Arc<RwLock<Dht>>, directory: String,
    items_completed: Arc<AtomicU32>, precision: usize,
    receiver: Receiver<Record>) -> Result<(), Box<dyn Error>> {
    // iterate over records
    loop {
        let record: Record = match receiver.recv() {
            Ok(record) => record,
            Err(_) => break,
        };

        // open image
        let filename = format!("{}/{}", directory, record.product_id);
        let mut reader = ImageReader::open(filename)?;
        reader.set_format(ImageFormat::Jpeg);

        let image = reader.decode()?;

        // initialize spatiotemporal image
        let lat_min = record.ll_lat.min(record.lr_lat);
        let lat_max = record.ul_lat.max(record.ur_lat);
        let long_min = record.ll_long.min(record.ul_long);
        let long_max = record.lr_long.max(record.ur_long);

        let mut raw_image = StImage::new(image,
            lat_min, lat_max, long_min, long_max, None);

        // split image with geohash precision
        for st_image in raw_image.split(precision) {
            // compute geohash hash
            let mut hasher = DefaultHasher::new();
            hasher.write(st_image.geohash().unwrap().as_bytes());
            let hash = hasher.finish();

            // discover hash location
            let addr = {
                let dht = dht.read().unwrap(); 
                let (node_id, addrs) = match dht.locate(hash) {
                    Some(node) => node,
                    None => {
                        warn!("no dht location for hash {}", hash);
                        continue;
                    },
                };

                match addrs.1 {
                    Some(addr) => addr.clone(),
                    None => {
                        warn!("dht node {} has no xfer_addr", node_id);
                        continue;
                    },
                }
            };

            // send image to new host
            if let Err(e) = crate::transfer::send_image(&record.spacecraft_id, 
                    &record.product_id, &st_image, &addr) {
                warn!("failed to write image to node {}: {}", addr, e);
            }
        }

        // increment items completed counter
        items_completed.fetch_add(1, Ordering::SeqCst);
    }

    Ok(())
}
