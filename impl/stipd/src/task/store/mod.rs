use swarm::prelude::Dht;

mod modis;
mod naip;
mod sentinel_2;

use crate::album::Album;
use crate::task::{Task, TaskHandle, TaskStatus};

use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Clone)]
pub enum ImageFormat {
    MODIS,
    NAIP,
    Sentinel,
}

pub struct StoreEarthExplorerTask {
    album: Arc<RwLock<Album>>,
    dht: Arc<RwLock<Dht>>,
    format: ImageFormat,
    glob: String,
    precision: usize,
    thread_count: u8,
}

impl StoreEarthExplorerTask {
    pub fn new(album: Arc<RwLock<Album>>, dht: Arc<RwLock<Dht>>,
            format: ImageFormat, glob: String, precision: usize,
            thread_count: u8) -> StoreEarthExplorerTask {
        StoreEarthExplorerTask {
            album: album,
            dht: dht,
            format: format,
            glob: glob,
            precision: precision,
            thread_count: thread_count,
        }
    }
}

#[tonic::async_trait]
impl Task for StoreEarthExplorerTask {
    async fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
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
            let album = self.album.clone();
            let dht_clone = self.dht.clone();
            let items_completed = items_completed.clone();
            let items_skipped = items_skipped.clone();
            let format = self.format.clone();
            let precision = self.precision.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                // iterate over records
                loop {
                    // fetch next record
                    let record: PathBuf = match receiver_clone.recv() {
                        Ok(record) => record,
                        Err(_) => break,
                    };

                    // process record
                    let result = match format {
                        ImageFormat::MODIS => modis::process(
                            &album, &dht_clone, precision, &record),
                        ImageFormat::NAIP => naip::process(
                            &album, &dht_clone, precision, &record),
                        ImageFormat::Sentinel => sentinel_2::process(
                            &album, &dht_clone, precision, &record),
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
