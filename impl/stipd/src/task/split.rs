use gdal::raster::Dataset;
use geohash::Coordinate;
use swarm::prelude::Dht;

use crate::album::Geocode;
use crate::image::{ImageManager, Image, StFile, RAW_SOURCE, SPLIT_SOURCE};
use crate::task::{Task, TaskHandle, TaskStatus};

use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct SplitTask {
    album: String,
    dht: Arc<RwLock<Dht>>,
    dht_key_length: i8,
    end_timestamp: Option<i64>,
    geocode: Geocode,
    geohash: Option<String>,
    geohash_bound: Option<String>,
    image_manager: Arc<RwLock<ImageManager>>,
    platform: Option<String>,
    precision: usize,
    recurse: bool,
    start_timestamp: Option<i64>,
    thread_count: u8,
}

impl SplitTask {
    pub fn new(album: String, dht: Arc<RwLock<Dht>>, dht_key_length: i8,
            end_timestamp: Option<i64>, geocode: Geocode,
            geohash: Option<String>, geohash_bound: Option<String>,
            image_manager: Arc<RwLock<ImageManager>>,
            platform: Option<String>, precision: usize, recurse: bool,
            start_timestamp: Option<i64>, thread_count: u8) -> SplitTask {
        SplitTask {
            album: album,
            dht: dht,
            dht_key_length: dht_key_length,
            end_timestamp: end_timestamp,
            geocode: geocode,
            geohash: geohash,
            geohash_bound: geohash_bound,
            image_manager: image_manager,
            platform: platform,
            precision: precision,
            recurse: recurse,
            start_timestamp: start_timestamp,
            thread_count: thread_count,
        }
    }
}

impl Task for SplitTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // search for images using ImageManager
        let mut records: Vec<(Image, Vec<StFile>)> = {
            let image_manager = self.image_manager.read().unwrap();
            image_manager.list(&self.end_timestamp,
                &self.geohash, &None, &None, &self.platform,
                self.recurse, &Some(RAW_SOURCE.to_string()),
                &self.start_timestamp)
        };

        // filter by geohash precision length
        records = records.into_iter().filter(|x| {
                (x.0).1.len() < self.precision as usize
            }).collect();

        // filter by result bounding geohash if necessary
        if let Some(geohash) = &self.geohash_bound {
            records = records.into_iter().filter(|(image, _)| {
                    image.1.starts_with(geohash)
                        || geohash.starts_with(&image.1)
                }).collect();
        }

        // initialize record channel
        let (sender, receiver) = crossbeam_channel::bounded(256);

        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let items_skipped = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let album_clone = self.album.clone();
            let dht_clone = self.dht.clone();
            let dht_key_length = self.dht_key_length.clone();
            let geocode = self.geocode.clone();
            let items_completed = items_completed.clone();
            let items_skipped = items_skipped.clone();
            let precision_clone = self.precision.clone();
            let receiver_clone = receiver.clone();

            // compute geohash intervals for given precision
            let (y_interval, x_interval) =
                st_image::prelude::get_geohash_intervals(self.precision);

            let join_handle = std::thread::spawn(move || {
                // iterate over records
                loop {
                    // fetch next record
                    let record = match receiver_clone.recv() {
                        Ok(record) => record,
                        Err(_) => break,
                    };

                    // process record
                    match process(&album_clone, &dht_clone,
                            dht_key_length, geocode, precision_clone,
                            &record, x_interval, y_interval) {
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

fn process(album: &str, dht: &Arc<RwLock<Dht>>, dht_key_length: i8,
        geocode: Geocode, precision: usize, record: &(Image, Vec<StFile>),
        x_interval: f64, y_interval: f64) -> Result<(), Box<dyn Error>> {
    let image = &record.0;
    for file in record.1.iter() {
        // check if path exists
        let path = Path::new(&file.0);
        if !path.exists() {
            return Err(format!("image path '{}' does not exist",
                path.to_string_lossy()).into());
        }

        // open image - TODO error
        let dataset = Dataset::open(&path).unwrap();

        // split image with geohash precision - TODO error
        for dataset_split in st_image::prelude::split(&dataset,
                4326, x_interval, y_interval).unwrap() {
            let (_, win_max_x, _, win_max_y) = dataset_split.coordinates();
            let coordinate = Coordinate{x: win_max_x, y: win_max_y};
            let geohash = geohash::encode(coordinate, precision)?;

            //  skip if geohash doesn't 'start_with' base image geohash
            if !geohash.starts_with(&image.1) {
                continue;
            }

            // perform dataset split - TODO error
            let dataset = dataset_split.dataset().unwrap();

            // if image has 0.0 coverage -> don't process - TODO error
            let pixel_coverage = st_image::coverage(&dataset).unwrap();
            if pixel_coverage == 0f64 {
                continue;
            }

            // lookup geohash in dht
            let addr = match crate::task::dht_lookup(
                    &dht, dht_key_length, &geohash) {
                Ok(addr) => addr,
                Err(e) => {
                    warn!("{}", e);
                    continue;
                },
            };

            // send image to new host
            if let Err(e) = crate::transfer::send_image(&addr, album,
                    &dataset, &geohash, file.1, &image.2,
                    SPLIT_SOURCE, file.2, &image.4, image.5) {
                warn!("failed to write image to node {}: {}", addr, e);
            }
        }
    }

    Ok(())
}
