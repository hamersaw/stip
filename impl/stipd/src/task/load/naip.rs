use chrono::prelude::{TimeZone, Utc};
use gdal::raster::Dataset;
use geohash::Coordinate;
use swarm::prelude::Dht;

use crate::image::RAW_SOURCE;

use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::ffi::OsStr;
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub fn process(dht: &Arc<RwLock<Dht>>, precision: usize, 
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
        let pixel_coverage = st_image::coverage(&dataset).unwrap();
        if pixel_coverage == 0f64 {
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
        if let Err(e) = crate::transfer::send_image(&addr, &dataset,
                "Base Image", &geohash, pixel_coverage, "NAIP",
                &RAW_SOURCE, 0, &tile, timestamp) {
            warn!("failed to write image to node {}: {}", addr, e);
        }
    }

    Ok(())
}

