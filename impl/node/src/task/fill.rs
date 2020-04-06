use crossbeam_channel::Receiver;

use crate::data::{DataManager, ImageMetadata};
use crate::task::{Task, TaskHandle, TaskStatus};

use std::cmp::Ordering;
use std::error::Error;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

pub struct FillTask {
    data_manager: Arc<DataManager>,
    geohash: String,
    platform: String,
    thread_count: u8,
    window_seconds: i64,
}

impl FillTask {
    pub fn new(data_manager: Arc<DataManager>, geohash: String,
            platform: String, thread_count: u8, window_seconds: i64)
            -> FillTask {
        FillTask {
            data_manager: data_manager,
            geohash: geohash,
            platform: platform,
            thread_count: thread_count,
            window_seconds: window_seconds,
        }
    }
}

impl Task for FillTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // search for images using DataManager
        let images = self.data_manager.search_images(
            &self.geohash, &self.platform)?;

        let mut filter_images: Vec<&ImageMetadata> = images.iter()
            .filter(|x| x.coverage != 1f64).collect();
        
        filter_images.sort_by(|a, b| 
            match a.geohash.cmp(&b.geohash) {
                Ordering::Equal => a.start_date.cmp(&b.start_date),
                x => x,
            });

        // initialize fill image vectors
        let mut records: Vec<Vec<ImageMetadata>> = Vec::new();
        let mut images_buf: Vec<ImageMetadata> = Vec::new();

        let mut geohash = "";
        let mut timestamp = 0i64;
        for image in filter_images {
            if image.geohash != geohash && image.start_date
                    - timestamp > self.window_seconds {
                // process images_buf
                if images_buf.len() >= 2 {
                    records.push(images_buf);
                    images_buf = Vec::new();
                } else {
                    images_buf.clear();
                }

                // reset geohash and timestamp
                geohash = &image.geohash;
                timestamp = image.start_date;
            }

            images_buf.push(image.clone());
        }

        // initialize record channel
        let (sender, receiver) = crossbeam_channel::bounded(256);

        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let items_skipped = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let items_completed = items_completed.clone();
            let items_skipped = items_skipped.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                if let Err(e) = worker_thread(items_completed,
                        items_skipped, receiver_clone) {
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

fn worker_thread(items_completed: Arc<AtomicU32>,
        _items_skipped: Arc<AtomicU32>,
        receiver: Receiver<Vec<ImageMetadata>>)
        -> Result<(), Box<dyn Error>> {
    // iterate over records
    loop {
        let record: Vec<ImageMetadata> = match receiver.recv() {
            Ok(record) => record,
            Err(_) => break,
        };

        // TODO - process
        println!("TODO - process images: {:?}", record);

        // increment items completed counter
        items_completed.fetch_add(1, AtomicOrdering::SeqCst);
    }

    Ok(())
}
