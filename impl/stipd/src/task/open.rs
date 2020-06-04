use gdal::metadata::Metadata;
use gdal::raster::Dataset;

use crate::album::Album;
use crate::task::{Task, TaskHandle, TaskStatus};

use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct OpenTask {
    album: Arc<RwLock<Album>>,
    thread_count: u8,
}

impl OpenTask {
    pub fn new(album: Arc<RwLock<Album>>, thread_count: u8) -> OpenTask {
        OpenTask {
            album: album,
            thread_count: thread_count,
        }
    }
}

impl Task for OpenTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // search for images using ImageManager
        let mut records: Vec<PathBuf> = {
            let album = self.album.read().unwrap();
            album.get_paths()?
        };

        // initialize record channel
        let (sender, receiver) = crossbeam_channel::bounded(256);

        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let items_skipped = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let album_clone = self.album.clone();
            let items_completed = items_completed.clone();
            let items_skipped = items_skipped.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                // iterate over records
                loop {
                    // fetch next record
                    let record = match receiver_clone.recv() {
                        Ok(record) => record,
                        Err(_) => break,
                    };

                    // process record
                    match process(&album_clone, &record) {
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

fn process(album: &Arc<RwLock<Album>>, record: &PathBuf)
        -> Result<(), Box<dyn Error>> {
    let dataset = Dataset::open(&record).unwrap();

    // TODO - error
    let cloud_coverage =
            match dataset.metadata_item("CLOUD_COVERAGE", "STIP") {
        Some(cloud_coverage) => Some(cloud_coverage.parse::<f64>()?),
        None => None,
    };
    let geohash = dataset.metadata_item("GEOHASH", "STIP").unwrap();
    let path = record.to_string_lossy().to_string();
    let pixel_coverage = dataset.metadata_item("PIXEL_COVERAGE", "STIP")
        .unwrap().parse::<f64>()?;
    let platform = dataset.metadata_item("PLATFORM", "STIP").unwrap();
    let source = dataset.metadata_item("SOURCE", "STIP").unwrap();
    let subdataset = dataset.metadata_item("SUBDATASET", "STIP")
        .unwrap().parse::<u8>()?;
    let tile = dataset.metadata_item("TILE", "STIP").unwrap();
    let timestamp = dataset.metadata_item("TIMESTAMP", "STIP")
        .unwrap().parse::<i64>()?;

    let mut album = album.write().unwrap();
    album.load(cloud_coverage, &geohash, pixel_coverage,
        &platform, &source, subdataset, &tile, timestamp)?;

    Ok(())
}
