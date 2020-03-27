use csv::Reader;
use image::ImageFormat;
use image::io::Reader as ImageReader;
use serde::Deserialize;
use st_image::StImage;

use crate::task::{Task, TaskHandle, TaskStatus};

use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct LoadEarthExplorerTask {
    directory: String,
    file: String,
    precision: usize,
    thread_count: u8,
}

impl LoadEarthExplorerTask {
    pub fn new(directory: String, file: String, precision: usize,
            thread_count: u8) -> LoadEarthExplorerTask {
        LoadEarthExplorerTask {
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
            let directory_clone = self.directory.clone();
            let items_completed = items_completed.clone();
            let precision_clone = self.precision.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                // iterate over records
                loop {
                    let result = receiver_clone.recv();
                    if let Err(_) = result {
                        break;
                    }

                    let record: Record = result.unwrap();

                    // open image
                    let filename = format!("{}/{}",
                        directory_clone, record.product_id);
                    let mut reader = match ImageReader::open(filename) {
                        Ok(reader) => reader,
                        Err(e) => panic!("{}", e),
                    };

                    reader.set_format(ImageFormat::Jpeg);

                    let image = match reader.decode() {
                        Ok(image) => image,
                        Err(e) => panic!("{}", e),
                    };

                    // initialize spatiotemporal image
                    let lat_min = record.ll_lat.min(record.lr_lat);
                    let lat_max = record.ul_lat.max(record.ur_lat);
                    let long_min = record.ll_long.min(record.ul_long);
                    let long_max = record.lr_long.max(record.ur_long);

                    let mut raw_image = StImage::new(image,
                        lat_min, lat_max, long_min, long_max, None);

                    // split image with geohash precision
                    for _st_image in raw_image.split(precision_clone) {
                        //println!("{:?} - {:?}", st_image.geohash(),
                        //    st_image.geohash_coverage());

                        // TODO - process image splits
                    }

                    // increment items completed counter
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
    product_id: String,
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
