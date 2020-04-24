use crossbeam_channel::Receiver;
use gdal::raster::Dataset;
use geohash::Coordinate;
use swarm::prelude::Dht;

use crate::image::{ImageManager, ImageMetadata};
use crate::task::{Task, TaskHandle, TaskStatus};

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::Hasher;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

pub struct SplitTask {
    band: String,
    dataset: String,
    dht: Arc<RwLock<Dht>>,
    geohash: String,
    image_manager: Arc<ImageManager>,
    platform: String,
    precision: usize,
    thread_count: u8,
}

impl SplitTask {
    pub fn new(band: String, dataset: String, dht: Arc<RwLock<Dht>>,
            geohash: String, image_manager: Arc<ImageManager>,
            platform: String, precision: usize,
            thread_count: u8) -> SplitTask {
        SplitTask {
            band: band,
            dataset: dataset,
            dht: dht,
            geohash: geohash,
            image_manager: image_manager,
            platform: platform,
            precision: precision,
            thread_count: thread_count,
        }
    }
}

impl Task for SplitTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // search for images using ImageManager
        let records = self.image_manager.search(&self.band,
            &self.dataset, &self.geohash, &self.platform)?;

        // initialize record channel
        let (sender, receiver) = crossbeam_channel::bounded(256);

        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let items_skipped = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let dht_clone = self.dht.clone();
            let items_completed = items_completed.clone();
            let items_skipped = items_skipped.clone();
            let precision_clone = self.precision.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                if let Err(e) = worker_thread(dht_clone,
                        items_completed, items_skipped, 
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
                items_skipped,
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

fn worker_thread(dht: Arc<RwLock<Dht>>, items_completed: Arc<AtomicU32>,
        items_skipped: Arc<AtomicU32>, precision: usize,
        receiver: Receiver<ImageMetadata>) -> Result<(), Box<dyn Error>> {
    // compute geohash intervals for given precision
    let (y_interval, x_interval) =
        st_image::coordinate::get_geohash_intervals(precision);

    // iterate over records
    loop {
        let record: ImageMetadata = match receiver.recv() {
            Ok(record) => record,
            Err(_) => break,
        };

        println!("PROCESSING RECORD: {:?}", record);

        // check if path exists
        let path = Path::new(&record.path);
        if !path.exists() {
            // increment items skipped counter
            items_skipped.fetch_add(1, AtomicOrdering::SeqCst);
            continue;
        }

        // open image - TODO error
        let dataset = Dataset::open(&path).unwrap();
 
        // split image with geohash precision - TODO error
        for (dataset, _, win_max_x, _, win_max_y) in st_image::split(
                &dataset, 4326, x_interval, y_interval).unwrap() {
            // compute window geohash
            let coordinate = Coordinate{x: win_max_x, y: win_max_y};
            let geohash = geohash::encode(coordinate, precision)?;

            // TODO - skip if geohash doesn't 'start_with' base image geohash
            println!("FOUND GEOHASH: {}", geohash);

            // compute geohash hash
            let mut hasher = DefaultHasher::new();
            hasher.write(geohash.as_bytes());
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

            // if image has 0.0 coverage -> don't process - TODO error
            let coverage = st_image::coverage(&dataset).unwrap();
            if coverage == 0f64 {
                continue;
            }

            // send image to new host
            let tile_id = &path.file_name().unwrap().to_string_lossy();
            if let Err(e) = crate::transfer::send_image(&record.platform, 
                    &geohash, &record.band, &tile_id, record.start_date,
                    record.end_date,  coverage, &dataset, &addr) {
                warn!("failed to write image to node {}: {}", addr, e);
            }
        }

        // increment items completed counter
        items_completed.fetch_add(1, AtomicOrdering::SeqCst);
    }

    Ok(())
}
