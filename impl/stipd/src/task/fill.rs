use crossbeam_channel::Receiver;
use gdal::raster::{Dataset, Driver};

use crate::image::{RAW_DATASET, FILLED_DATASET, ImageManager, ImageMetadata};
use crate::task::{Task, TaskHandle, TaskStatus};

use std::cmp::Ordering;
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};

pub struct FillTask {
    band: String,
    geohash: String,
    image_manager: Arc<ImageManager>,
    platform: String,
    thread_count: u8,
    window_seconds: i64,
}

impl FillTask {
    pub fn new(band: String, geohash: String,
            image_manager: Arc<ImageManager>, platform: String,
            thread_count: u8, window_seconds: i64) -> FillTask {
        FillTask {
            band: band,
            geohash: geohash,
            image_manager: image_manager,
            platform: platform,
            thread_count: thread_count,
            window_seconds: window_seconds,
        }
    }
}

impl Task for FillTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // search for images using ImageManager
        let images = self.image_manager.search(&self.band,
            RAW_DATASET, &self.geohash, &self.platform)?;

        let mut filter_images: Vec<&ImageMetadata> = images.iter()
            .filter(|x| x.coverage != 1f64).collect();

        // order by platform, geohash, band
        filter_images.sort_by(|a, b| {
            let platform_cmp = a.platform.cmp(&b.platform);
            if platform_cmp != Ordering::Equal {
                return platform_cmp;
            }

            let geohash_cmp = a.geohash.cmp(&b.geohash);
            if geohash_cmp != Ordering::Equal {
                return geohash_cmp;
            }

            let band_cmp = a.band.cmp(&b.band);
            if band_cmp != Ordering::Equal {
                return band_cmp;
            }

            a.start_date.cmp(&b.start_date)
        });

        // initialize fill image vectors
        let mut records: Vec<Vec<ImageMetadata>> = Vec::new();
        let mut images_buf: Vec<ImageMetadata> = Vec::new();

        let mut platform = "";
        let mut geohash = "";
        let mut band = "";
        let mut timestamp = 0i64;
        for image in filter_images {
            if image.platform != platform || image.geohash != geohash
                    || image.band != band || image.start_date
                        - timestamp > self.window_seconds {
                // process images_buf
                if images_buf.len() >= 2 {
                    records.push(images_buf);
                    images_buf = Vec::new();
                } else {
                    images_buf.clear();
                }

                // reset geohash and timestamp
                platform = &image.platform;
                geohash = &image.geohash;
                band = &image.band;
                timestamp = image.start_date;
            }

            images_buf.push(image.clone());
        }
        
        if images_buf.len() >= 2 {
            records.push(images_buf);
        }

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
                if let Err(e) = worker_thread(image_manager,
                        items_completed, items_skipped, receiver_clone) {
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

fn worker_thread(image_manager: Arc<ImageManager>,
        items_completed: Arc<AtomicU32>, items_skipped: Arc<AtomicU32>,
        receiver: Receiver<Vec<ImageMetadata>>) -> Result<(), Box<dyn Error>> {
    // iterate over records
    loop {
        let record: Vec<ImageMetadata> = match receiver.recv() {
            Ok(record) => record,
            Err(_) => break,
        };

        // check if path exists
        let path = Path::new(&record[0].path);
        if !path.exists() {
            // increment items skipped counter
            items_skipped.fetch_add(1, AtomicOrdering::SeqCst);
            continue;
        }

        // open image - TODO error
        let dataset = Dataset::open(&path).unwrap();
 
        // read dataset rasterbands
        let mut rasters = Vec::new();
        for i in 0..dataset.count() {
            let raster = dataset.read_full_raster_as::<u8>(i+1).unwrap();
            rasters.push(raster);
        }

        for i in 1..record.len() {
            let image = &record[i];

            // check if path exists
            let fill_path = Path::new(&image.path);
            if !fill_path.exists() {
                // TODO - log
                continue;
            }

            // open fill image - TODO  error
            let fill_dataset = Dataset::open(&fill_path).unwrap();
 
            // read fill dataset rasterbands
            let mut fill_rasters = Vec::new();
            for i in 0..fill_dataset.count() {
                let fill_raster = fill_dataset
                    .read_full_raster_as::<u8>(i+1).unwrap();
                fill_rasters.push(fill_raster);
            }

            // fill rasterband
            st_image::fill(&mut rasters, &fill_rasters)?;
        }
 
        // open memory dataset
        let (width, height) = dataset.size();
        let driver = Driver::get("Mem").unwrap();
        let mem_dataset = driver.create("unreachable", width as isize,
            height as isize, rasters.len() as isize).unwrap();

        mem_dataset.set_geo_transform(
            &dataset.geo_transform().unwrap()).unwrap();
        mem_dataset.set_projection(
            &dataset.projection()).unwrap();

        // set rasterbands - TODO error
        for (i, raster) in rasters.iter().enumerate() {
            mem_dataset.write_raster((i + 1) as isize,
                (0, 0), (width, height), &raster).unwrap();
        }

        // write mem_dataset - TODO error
        let image = &record[0];
        let tile_id = &path.file_name().unwrap().to_string_lossy();
        let coverage = st_image::coverage(&mem_dataset).unwrap();

        if coverage > image.coverage {
            image_manager.write(&image.platform, &image.geohash, 
                &image.band, FILLED_DATASET, &tile_id, image.start_date,
                image.end_date, coverage, &mem_dataset)?;
        }

        // increment items completed counter
        items_completed.fetch_add(1, AtomicOrdering::SeqCst);
    }

    Ok(())
}
