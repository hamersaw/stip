use swarm::prelude::Dht;

mod modis;
mod naip;
mod sentinel_2;

use crate::task::{Task, TaskHandle, TaskStatus};

use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Clone)]
pub enum LoadFormat {
    MODIS,
    NAIP,
    Sentinel,
}

pub struct LoadEarthExplorerTask {
    dht: Arc<RwLock<Dht>>,
    glob: String,
    load_format: LoadFormat,
    precision: usize,
    thread_count: u8,
}

impl LoadEarthExplorerTask {
    pub fn new(dht: Arc<RwLock<Dht>>, glob: String,
            load_format: LoadFormat, precision: usize,
            thread_count: u8) -> LoadEarthExplorerTask {
        LoadEarthExplorerTask {
            dht: dht,
            glob: glob,
            load_format: load_format,
            precision: precision,
            thread_count: thread_count,
        }
    }
}

impl Task for LoadEarthExplorerTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // search for image files
        let mut records = Vec::new();
        for entry in glob::glob(&self.glob)? {
            records.push(entry?);
        }

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
            let load_format = self.load_format.clone();
            let precision = self.precision.clone();
            let receiver_clone = receiver.clone();

            // compute geohash intervals for given precision
            let (y_interval, x_interval) =
                st_image::prelude::get_geohash_intervals(self.precision);

            let join_handle = std::thread::spawn(move || {
                // iterate over records
                loop {
                    // fetch next record
                    let record: PathBuf = match receiver_clone.recv() {
                        Ok(record) => record,
                        Err(_) => break,
                    };

                    // process record
                    let result = match load_format {
                        LoadFormat::MODIS => modis::process(
                            &dht_clone, precision, &record,
                            x_interval, y_interval),
                        LoadFormat::NAIP => naip::process(
                            &dht_clone, precision, &record,
                            x_interval, y_interval),
                        LoadFormat::Sentinel => sentinel_2::process(
                            &dht_clone, precision, &record,
                            x_interval, y_interval),
                    };

                    // process result
                    match result {
                        Ok(_) => items_completed.fetch_add(1, Ordering::SeqCst),
                        Err(e) => {
                            warn!("skipping record '{}': {}",
                                &record.to_string_lossy(), e);
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
