use failure::ResultExt;
use gdal::raster::Dataset;
use swarm::prelude::Dht;

use crate::{Image, StFile, RAW_SOURCE, SPLIT_SOURCE};
use crate::album::Album;
use crate::task::{TaskOg, TaskHandle, TaskStatus};

use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct SplitTask {
    album: Arc<RwLock<Album>>,
    dht: Arc<RwLock<Dht>>,
    end_timestamp: Option<i64>,
    geocode: Option<String>,
    geocode_bound: Option<String>,
    platform: Option<String>,
    precision: usize,
    recurse: bool,
    start_timestamp: Option<i64>,
    thread_count: u8,
}

impl SplitTask {
    pub fn new(album: Arc<RwLock<Album>>, dht: Arc<RwLock<Dht>>,
            end_timestamp: Option<i64>, geocode: Option<String>,
            geocode_bound: Option<String>, platform: Option<String>,
            precision: usize, recurse: bool,
            start_timestamp: Option<i64>, thread_count: u8) -> SplitTask {
        SplitTask {
            album: album,
            dht: dht,
            end_timestamp: end_timestamp,
            geocode: geocode,
            geocode_bound: geocode_bound,
            platform: platform,
            precision: precision,
            recurse: recurse,
            start_timestamp: start_timestamp,
            thread_count: thread_count,
        }
    }
}

#[tonic::async_trait]
impl TaskOg for SplitTask {
    async fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // search for images using Album
        let mut records: Vec<(Image, Vec<StFile>)> = {
            let album = self.album.read().unwrap();
            album.list(&self.end_timestamp, &self.geocode, &None, &None,
                &self.platform, self.recurse, 
                &Some(RAW_SOURCE.to_string()), &self.start_timestamp)?
        };

        // filter by geocode precision length
        records = records.into_iter().filter(|x| {
                (x.0).1.len() < self.precision as usize
            }).collect();

        // filter by result bounding geocode if necessary
        if let Some(geocode) = &self.geocode_bound {
            records = records.into_iter().filter(|(image, _)| {
                    image.1.starts_with(geocode)
                        || geocode.starts_with(&image.1)
                }).collect();
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
            let precision_clone = self.precision.clone();
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
                    match process(&album, &dht_clone,
                            precision_clone, &record) {
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

fn process(album: &Arc<RwLock<Album>>, dht: &Arc<RwLock<Dht>>,
        precision: usize, record: &(Image, Vec<StFile>))
        -> Result<(), Box<dyn Error>> {
    let image = &record.0;

    // retrieve album metadata
    let (album_id, dht_key_length, geocode) = {
        let album = album.read().unwrap();
        (album.get_id().to_string(), album.get_dht_key_length(),
            album.get_geocode().clone())
    };

    for file in record.1.iter() {
        // check if path exists
        let path = {
            let album = album.read().unwrap();
            Path::new(&file.0);
            album.get_image_path(false, &image.1,
                &image.2, &image.3, file.2, &image.4)?
        };

        if !path.exists() {
            return Err(format!("image path '{}' does not exist",
                path.to_string_lossy()).into());
        }

        // open image
        let dataset = Dataset::open(&path).compat()?;

        // split image with geocode precision
        for dataset_split in st_image::prelude::split(&dataset,
                geocode, precision).compat()? {
            // calculate split dataset geocode
            let (win_min_x, win_max_x, win_min_y, win_max_y) =
                dataset_split.coordinates();
            let split_geocode = geocode.get_code(
                (win_min_x + win_max_x) / 2.0,
                (win_min_y + win_max_y) / 2.0, precision)?;

            //  skip if geocode doesn't 'start_with' base image geocode
            if !split_geocode.starts_with(&image.1) {
                continue;
            }

            // perform dataset split
            let dataset = dataset_split.dataset().compat()?;

            // if image has 0.0 coverage -> don't process
            let pixel_coverage = st_image::coverage(&dataset).compat()?;
            if pixel_coverage == 0f64 {
                continue;
            }

            // lookup geocode in dht
            let addr = match crate::task::dht_lookup(
                    &dht, dht_key_length, &split_geocode) {
                Ok(addr) => addr,
                Err(e) => {
                    warn!("{}", e);
                    continue;
                },
            };

            // send image to new host
            if let Err(e) = crate::transfer::send_image(&addr, &album_id,
                    &dataset, &split_geocode, file.1, &image.2,
                    SPLIT_SOURCE, file.2, &image.4, image.5) {
                warn!("failed to write image to node {}: {}", addr, e);
            }
        }
    }

    Ok(())
}
