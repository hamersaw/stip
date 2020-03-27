use csv::Reader;
use serde::Deserialize;

use crate::task::{Task, TaskHandle, TaskStatus};

use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct LoadEarthExplorerTask {
    channels: Vec<String>,
    directory: String,
    file: String,
    satellite: String,
    thread_count: u8,
}

impl LoadEarthExplorerTask {
    pub fn new(file: String) -> LoadEarthExplorerTask { // TODO - pass arguments
        LoadEarthExplorerTask {
            channels: Vec::new(),
            directory: String::from(""),
            file: file,
            satellite: String::from(""),
            thread_count: 2,
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
            let items_completed = items_completed.clone();
            let receiver_clone = receiver.clone();
            let join_handle = std::thread::spawn(move || {
                // iterate over records
                loop {
                    let result = receiver_clone.recv();
                    if let Err(_) = result {
                        break;
                    }

                    // TODO - process record
                    println!("TODO process {:?}", result.unwrap());
                    std::thread::sleep(std::time::Duration::from_millis(500));

                    items_completed.fetch_add(1, Ordering::SeqCst);
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
    landsat_product_identifier: String,
    #[serde(rename(deserialize = "Landsat Scene Identifier"))]
    landsat_scene_identifier: String,
    #[serde(rename(deserialize = "Land Cloud Cover"))]
    land_cloud_cover: f32,
    #[serde(rename(deserialize = "Scene Cloud Cover"))]
    scene_cloud_cover: f32,
    #[serde(rename(deserialize = "LL Corner Lat dec"))]
    ll_corner_lat_dec: f64,
    #[serde(rename(deserialize = "LL Corner Long dec"))]
    ll_corner_long_dec: f64,
    #[serde(rename(deserialize = "UL Corner Lat dec"))]
    ul_corner_lat_dec: f64,
    #[serde(rename(deserialize = "UL Corner Long dec"))]
    ul_corner_long_dec: f64,
    #[serde(rename(deserialize = "LR Corner Lat dec"))]
    lr_corner_lat_dec: f64,
    #[serde(rename(deserialize = "LR Corner Long dec"))]
    lr_corner_long_dec: f64,
    #[serde(rename(deserialize = "UR Corner Lat dec"))]
    ur_corner_lat_dec: f64,
    #[serde(rename(deserialize = "UR Corner Long dec"))]
    ur_corner_long_dec: f64,
}
