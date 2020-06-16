use gdal::raster::Dataset;

use crate::FILLED_SOURCE;
use crate::album::AlbumManager;
use crate::task::{TaskOg, TaskHandleOg};

use std::cmp::Ordering as CmpOrdering;
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct FillTask {
    end_timestamp: Option<i64>,
    geocode: Option<String>,
    platform: Option<String>,
    recurse: bool,
    start_timestamp: Option<i64>,
    thread_count: u8,
    window_seconds: i64,
}

impl FillTask {
    pub fn new(end_timestamp: Option<i64>, geocode: Option<String>,
            platform: Option<String>, recurse: bool,
            start_timestamp: Option<i64>, thread_count: u8,
            window_seconds: i64) -> FillTask {
        FillTask {
            end_timestamp: end_timestamp,
            geocode: geocode,
            platform: platform,
            recurse: recurse,
            start_timestamp: start_timestamp,
            thread_count: thread_count,
            window_seconds: window_seconds,
        }
    }
}

#[tonic::async_trait]
impl TaskOg for FillTask {
    async fn start(&self) -> Result<Arc<RwLock<TaskHandleOg>>, Box<dyn Error>> {
        unimplemented!();
        /*// search for images using ImageManager
        let mut images: Vec<ImageMetadata> = {
            let image_manager = self.image_manager.read().unwrap();
            image_manager.list(&self.end_timestamp,
                &self.geocode, &None, &None, &self.platform,
                self.recurse, &None, &self.start_timestamp)
        };

        // order by platform, geocode, band
        images.sort_by(|a, b| {
            let platform_cmp = a.platform.cmp(&b.platform);
            if platform_cmp != CmpOrdering::Equal {
                return platform_cmp;
            }

            let geocode_cmp = a.geocode.cmp(&b.geocode);
            if geocode_cmp != CmpOrdering::Equal {
                return geocode_cmp;
            }

            a.timestamp.cmp(&b.timestamp)
        });

        // initialize fill image vectors
        let mut records: Vec<Vec<ImageMetadata>> = Vec::new();
        let mut images_buf: Vec<ImageMetadata> = Vec::new();

        let mut platform = "";
        let mut geocode = "";
        let mut timestamp = 0i64;
        for image in images.iter() {
            if image.platform != platform || image.geocode != geocode
                    || image.timestamp - timestamp > self.window_seconds {
                // process images_buf
                if images_buf.len() >= 2 {
                    records.push(images_buf);
                    images_buf = Vec::new();
                } else {
                    images_buf.clear();
                }

                // reset geocode and timestamp
                platform = &image.platform;
                geocode = &image.geocode;
                timestamp = image.timestamp;
            }

            images_buf.push(image.clone());
        }
        
        if images_buf.len() >= 2 {
            records.push(images_buf);
        }

        // filter out vectors where a full pixel coverage image exists
        let records: Vec<Vec<ImageMetadata>> = records.into_iter()
            .filter(|x| {
                let mut valid = true;
                for image in x {
                    valid = valid || image.pixel_coverage == 1f64;
                }

                valid
            }).collect();

        // initialize record channel
        let (sender, receiver) = crossbeam_channel::bounded(256);

        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let items_skipped = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let image_manager = self.image_manager.clone();
            let items_completed = items_completed.clone();
            let items_skipped = items_skipped.clone();
            let receiver_clone = receiver.clone();

            let join_handle = std::thread::spawn(move || {
                // iterate over records
                loop {
                    // fetch next record
                    let mut record: Vec<ImageMetadata> = 
                            match receiver_clone.recv() {
                        Ok(record) => record,
                        Err(_) => break,
                    };

                    // process record
                    match process(&image_manager, &mut record) {
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

        // initialize TaskHandleOg
        let task_handle = Arc::new( RwLock::new(
            TaskHandleOg::new(
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
                    // set TaskHandleOg status to 'failed'
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
                    // set TaskHandleOg status to 'failed'
                    let mut task_handle =
                        task_handle_clone.write().unwrap();
                    task_handle.set_status(
                        TaskStatus::Failure(format!("{:?}", e)));

                    return;
                }
            }

            // set TaskHandleOg status to 'completed'
            let mut task_handle = task_handle_clone.write().unwrap();
            task_handle.set_status(TaskStatus::Complete);
        });

        // return task handle
        Ok(task_handle)*/
    }
}

/*fn process(image_manager: &Arc<RwLock<ImageManager>>,
        record: &mut Vec<ImageMetadata>) -> Result<(), Box<dyn Error>> {
    // sort records by pixel_coverage
    record.sort_by(|a, b| {
        if a.pixel_coverage > b.pixel_coverage {
            CmpOrdering::Greater
        } else if a.pixel_coverage > b.pixel_coverage {
            CmpOrdering::Less
        } else {
            CmpOrdering::Equal
        }
    });

    // read datasets
    let mut datasets = Vec::new();
    for image in record.iter() {
        // TODO check if path exists
        let path = Path::new(&image.path);
        if !path.exists() {
            // TODO - log
            continue;
        }

        // open image - TODO  error
        let dataset = Dataset::open(&path).unwrap();
        datasets.push(dataset);
    }

    // perform fill - TODO error
    let mut dataset = st_image::prelude::fill(&datasets).unwrap();
    let pixel_coverage = st_image::coverage(&dataset).unwrap();

    // check if pixel coverage is more than previous highest
    let mut max_pixel_coverage = 0f64;
    for image in record.iter() {
        if image.pixel_coverage > max_pixel_coverage {
            max_pixel_coverage = image.pixel_coverage;
        }
    }

    if pixel_coverage > max_pixel_coverage {
        // TODO - write mem_dataset - TODO error
        let image = &record[0];
        let path = Path::new(&record[0].path);
        let tile_id = &path.file_name().unwrap().to_string_lossy();

        let mut image_manager = image_manager.write().unwrap();
        image_manager.write(&image.platform, &image.geocode, 
            FILLED_SOURCE, &tile_id, image.timestamp,
            pixel_coverage, &mut dataset)?;
    }

    Ok(())
}*/
