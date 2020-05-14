use chrono::prelude::{DateTime, TimeZone, Utc};
use gdal::metadata::Metadata;
use gdal::raster::Dataset;
use geohash::Coordinate;
use swarm::prelude::Dht;
use zip::ZipArchive;

use crate::task::{Task, TaskHandle, TaskStatus};

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::ffi::OsStr;
use std::fs::File;
use std::hash::Hasher;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

#[derive(Clone)]
pub enum LoadFormat {
    NAIP,
    Sentinel,
}

pub struct LoadEarthExplorerTask {
    dht: Arc<RwLock<Dht>>,
    directory: String,
    load_format: LoadFormat,
    precision: usize,
    thread_count: u8,
}

impl LoadEarthExplorerTask {
    pub fn new(dht: Arc<RwLock<Dht>>, directory: String,
            load_format: LoadFormat, precision: usize,
            thread_count: u8) -> LoadEarthExplorerTask {
        LoadEarthExplorerTask {
            dht: dht,
            directory: directory,
            load_format: load_format,
            precision: precision,
            thread_count: thread_count,
        }
    }
}

impl Task for LoadEarthExplorerTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // read file records
        let directory = format!("/{}*", self.directory);

        // search for image files
        let mut records = Vec::new();
        for entry in glob::glob(&directory)? {
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
                        LoadFormat::NAIP => process_naip(&dht_clone,
                            precision, &record, x_interval, y_interval),
                        LoadFormat::Sentinel => process_sentinel(
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

pub fn process_naip(dht: &Arc<RwLock<Dht>>, precision: usize, 
        record: &PathBuf, x_interval: f64, y_interval: f64)
        -> Result<(), Box<dyn Error>> {
    // open geotiff file
    let tif_path = record.with_extension("tif");
    let filename = tif_path.file_name().unwrap()
        .to_string_lossy().to_lowercase();

    let image_path = PathBuf::from(format!("/vsizip/{}/{}",
        record.to_string_lossy(), filename));
    let dataset = Dataset::open(&image_path)
        .expect("metadata dataset open");

    // parse metadata
    let date_string = &filename[filename.len()-12..filename.len()-4];
    let year = date_string[0..4].parse::<i32>()?;
    let month = date_string[4..6].parse::<u32>()?;
    let day = date_string[6..8].parse::<u32>()?;
    let datetime = Utc.ymd(year, month, day).and_hms(0, 0, 0);

    let timestamp = datetime.timestamp();

    let tile_path = record.with_extension("");
    let tile = tile_path.file_name()
        .unwrap_or(OsStr::new("")).to_string_lossy();

    // split image with geohash precision - TODO error
    for (dataset, _, win_max_x, _, win_max_y) in st_image::prelude::split(
            &dataset, 4326, x_interval, y_interval).unwrap() {
        // compute window geohash
        let coordinate = Coordinate{x: win_max_x, y: win_max_y};
        let geohash = geohash::encode(coordinate, precision)
            .expect("compute window geohash");

        // if image has 0.0 coverage -> don't process - TODO error
        let pixel_coverage =
            st_image::coverage(&dataset).unwrap() as f32;
        if pixel_coverage == 0f32 {
            continue;
        }

        //println!("{} {} {}", tile, geohash, pixel_coverage);

        // compute geohash hash
        let mut hasher = DefaultHasher::new();
        hasher.write(geohash.as_bytes());
        let hash = hasher.finish();

        // discover hash location
        let addr = {
            let dht = dht.read().unwrap(); 
            let (node_id, addrs) = match dht.locate(hash) {
                Some(node) => node,
                None => {
                    warn!("no dht location for hash {}", hash);
                    continue;
                },
            };

            match addrs.1 {
                Some(addr) => addr.clone(),
                None => {
                    warn!("dht node {} has no xfer_addr", node_id);
                    continue;
                },
            }
        };

        // send image to new host
        if let Err(e) = crate::transfer::send_image("NAIP",
                &geohash, "TCI", &tile, timestamp,
                pixel_coverage, &dataset, &addr) {
            warn!("failed to write image to node {}: {}", addr, e);
        }
    }

    Ok(())
}

pub fn process_sentinel(dht: &Arc<RwLock<Dht>>, precision: usize, 
        record: &PathBuf, x_interval: f64, y_interval: f64)
        -> Result<(), Box<dyn Error>> {
    // compute tile name
    let tile_path = record.with_extension("");
    let tile = tile_path.file_name()
        .unwrap_or(OsStr::new("")).to_string_lossy();

    //println!("TILE: '{}'", tile);

    // open zip archive
    let file = File::open(&record)?;
    let reader = BufReader::new(file);
    let archive = ZipArchive::new(reader)?;

    // identify metadata xml file and band image files
    let mut zip_images = Vec::new();
    let mut zip_metadata_option = None;
    for filename in archive.file_names() {
        let path = PathBuf::from(&filename);

        if path.extension() == Some(OsStr::new("jp2")) {
            zip_images.push(filename);
        } else if path.file_name() == Some(OsStr::new("MTD_MSIL1C.xml")) {
            zip_metadata_option = Some(filename);
        }
    }

    // check if we identified xml metadata file and band image files
    if zip_metadata_option == None {
        return Err("unable to find xml metadata file".into());
    } else if zip_images.len() == 0 {
        return Err("no band images found".into());
    }
    
    // open gdal metadata dataset - TODO error
    let zip_metadata = zip_metadata_option.unwrap();
    let metadata_filename = format!("/vsizip/{}/{}",
        record.to_string_lossy(), zip_metadata);
    let metadata_path = PathBuf::from(&metadata_filename);
    let dataset = Dataset::open(&metadata_path).unwrap();

    // parse metadata
    let platform = match dataset.metadata_item("DATATAKE_1_SPACECRAFT_NAME", "") {
        Some(platform) => platform,
        None => return Err("spacecraft metadata not found".into()),
    };

    let timestamp = match dataset.metadata_item("PRODUCT_START_TIME", "") {
        Some(time) => time.parse::<DateTime<Utc>>()?.timestamp(),
        None => return Err("start time metadata not found".into()),
    };

    //let uri = dataset.metadata_item("PRODUCT_URI", ""));
    //println!("  PLATFORM: {}", platform);
    //println!("  TIMES: {} - {}", start_time, end_time);

    // iterate over zipped images
    for zip_image in zip_images.iter() {
        // open dataset - TODO error
        let zip_image_filename = format!("/vsizip/{}/{}",
            record.to_string_lossy(), zip_image);
        let zip_image_path = PathBuf::from(&zip_image_filename);
        let dataset = Dataset::open(&zip_image_path).unwrap();

        // parse band ID
        let band = &zip_image[zip_image.len() - 7..zip_image.len() - 4];
        //println!("  BAND: {}", band_id);

        // split image with geohash precision - TODO error
        for (dataset, _, win_max_x, _, win_max_y) in
                st_image::prelude::split(&dataset, 4326,
                    x_interval, y_interval).unwrap() {
            // compute window geohash
            let coordinate = Coordinate{x: win_max_x, y: win_max_y};
            let geohash = geohash::encode(coordinate, precision)?;

            // if image has 0.0 coverage -> don't process - TODO error
            let pixel_coverage =
                st_image::coverage(&dataset).unwrap() as f32;
            if pixel_coverage == 0f32 {
                continue;
            }

            // compute geohash hash
            let mut hasher = DefaultHasher::new();
            hasher.write(geohash.as_bytes());
            let hash = hasher.finish();

            // discover hash location
            let addr = {
                let dht = dht.read().unwrap(); 
                let (node_id, addrs) = match dht.locate(hash) {
                    Some(node) => node,
                    None => {
                        warn!("no dht location for hash {}", hash);
                        continue;
                    },
                };

                match addrs.1 {
                    Some(addr) => addr.clone(),
                    None => {
                        warn!("dht node {} has no xfer_addr", node_id);
                        continue;
                    },
                }
            };

            //println!("    GEOHASH: {}", geohash);
            //println!("    DIMENSIONS: {:?}", dataset.size());

            // send image to new host
            if let Err(e) = crate::transfer::send_image(&platform,
                    &geohash, &band, &tile, timestamp,
                    pixel_coverage, &dataset, &addr) {
                warn!("failed to write image to node {}: {}", addr, e);
            }
        }
    }

    Ok(())
}
