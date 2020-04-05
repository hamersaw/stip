use csv::Reader;
use crossbeam_channel::Receiver;
use gdal::raster::Dataset;
use image::io::Reader as ImageReader;
use serde::Deserialize;
use swarm::prelude::Dht;

use crate::task::{Task, TaskHandle, TaskStatus};

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fs::File;
use std::hash::Hasher;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct LoadEarthExplorerTask {
    dht: Arc<RwLock<Dht>>,
    directory: String,
    file: String,
    load_format: LoadFormat,
    precision: usize,
    thread_count: u8,
}

impl LoadEarthExplorerTask {
    pub fn new(dht: Arc<RwLock<Dht>>, directory: String,
            file: String, load_format: LoadFormat,
            precision: usize, thread_count: u8) -> LoadEarthExplorerTask {
        LoadEarthExplorerTask {
            dht: dht,
            directory: directory,
            file: file,
            load_format: load_format,
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
        let records = self.load_format.records(&mut reader)?;

        // initialize record channel
        let (sender, receiver) = crossbeam_channel::bounded(256);

        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let items_skipped = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let dht_clone = self.dht.clone();
            let directory_clone = self.directory.clone();
            let items_completed = items_completed.clone();
            let items_skipped = items_skipped.clone();
            let precision_clone = self.precision.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                if let Err(e) = worker_thread(dht_clone,
                        directory_clone, items_completed, items_skipped,
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

fn worker_thread(dht: Arc<RwLock<Dht>>, directory: String,
        items_completed: Arc<AtomicU32>, items_skipped: Arc<AtomicU32>,
        precision: usize, receiver: Receiver<Record>) 
        -> Result<(), Box<dyn Error>> {
    // iterate over records
    loop {
        let record: Record = match receiver.recv() {
            Ok(record) => record,
            Err(_) => break,
        };

        // check if path exists
        let filename = format!("{}/{}", directory, record.tile());
        let path = Path::new(&filename);
        if !path.exists() {
            // increment items skipped counter
            items_skipped.fetch_add(1, Ordering::SeqCst);
            continue;
        }

        // open image - TODO error
        let dataset = Dataset::open(&path).unwrap();
        // TODO - process imageformat (when it exists)

        // split image with geohash precision - TODO error
        for (geohash, dataset) in
                st_image::split(&dataset, precision).unwrap() {
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

            // send image to new host
            if let Err(e) = crate::transfer::send_image(&record.platform(), 
                    &geohash, &record.tile(), &dataset, &addr) {
                warn!("failed to write image to node {}: {}", addr, e);
            }
        }

        // increment items completed counter
        items_completed.fetch_add(1, Ordering::SeqCst);
    }

    Ok(())
}

pub enum LoadFormat {
    Landsat,
    Sentinel,
}

impl LoadFormat {
    fn records(&self, reader: &mut Reader<File>)
            -> Result<Vec<Record>, Box<dyn Error>> {
        let mut records = Vec::new();
        match self {
            LoadFormat::Landsat => {
                // parse all records as 'landsat'
                for result in reader.deserialize() {
                    let record: LandsatRecord = result?;
                    records.push(Record::Landsat(record));
                }
            },
            LoadFormat::Sentinel => {
                // parse all records as 'sentinel'
                for result in reader.deserialize() {
                    let record: SentinelRecord = result?;
                    records.push(Record::Sentinel(record));
                }
            },
        }

        Ok(records)
    }
}

enum Record {
    Landsat(LandsatRecord),
    Sentinel(SentinelRecord),
}

impl Record {
    fn bounds(&self) -> (f64, f64, f64, f64) {
        match self {
            Record::Landsat(r) => {
                (r.ll_lat.min(r.lr_lat), r.ul_lat.max(r.ur_lat),
                    r.ll_long.min(r.ul_long), r.lr_long.max(r.ur_long))
            },
            Record::Sentinel(r) => {
                (r.se_lat.min(r.sw_lat), r.ne_lat.max(r.nw_lat),
                    r.sw_long.min(r.nw_long), r.se_long.max(r.ne_long))
            },
        }
    }

    fn platform(&self) -> &str {
        match self {
            Record::Landsat(record) => &record.spacecraft_id,
            Record::Sentinel(record) => &record.platform,
        }
    }

    fn tile(&self) -> &str {
        match self {
            Record::Landsat(record) => &record.product_id,
            Record::Sentinel(record) => &record.vendor_tile_id,
        }
    }
}

#[derive(Debug, Deserialize)]
struct LandsatRecord {
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

#[derive(Debug, Deserialize)]
struct SentinelRecord {
    #[serde(rename(deserialize = "Vendor Tile ID"))]
    vendor_tile_id: String,
    #[serde(rename(deserialize = "Platform"))]
    platform: String,
    #[serde(rename(deserialize = "SW Corner Lat dec"))]
    sw_lat: f64,
    #[serde(rename(deserialize = "SW Corner Long dec"))]
    sw_long: f64,
    #[serde(rename(deserialize = "NW  Corner Lat dec"))]
    nw_lat: f64,
    #[serde(rename(deserialize = "NW Corner Long dec"))]
    nw_long: f64,
    #[serde(rename(deserialize = "SE Corner Lat dec"))]
    se_lat: f64,
    #[serde(rename(deserialize = "SE Corner Long dec"))]
    se_long: f64,
    #[serde(rename(deserialize = "NE Corner Lat dec"))]
    ne_lat: f64,
    #[serde(rename(deserialize = "NE Corner Long dec"))]
    ne_long: f64,
}
