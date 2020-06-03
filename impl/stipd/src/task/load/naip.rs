use chrono::prelude::{TimeZone, Utc};
use gdal::raster::Dataset;
use geohash::Coordinate;
use swarm::prelude::Dht;

use crate::image::RAW_SOURCE;

use std::error::Error;
use std::ffi::OsStr;
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
    for dataset_split in st_image::prelude::split(&dataset,
            4326, x_interval, y_interval).unwrap() {
        let (_, win_max_x, _, win_max_y) = dataset_split.coordinates();
        let coordinate = Coordinate{x: win_max_x, y: win_max_y};
        let geohash = geohash::encode(coordinate, precision)?;

        // perform dataset split - TODO error
        let dataset = dataset_split.dataset().unwrap();

        // if image has 0.0 coverage -> don't process - TODO error
        let pixel_coverage = st_image::coverage(&dataset).unwrap();
        if pixel_coverage == 0f64 {
            continue;
        }

        //println!("{} {} {}", tile, geohash, pixel_coverage);

        // lookup geohash in dht
        let addr = match crate::task::dht_lookup(&dht, &geohash) {
            Ok(addr) => addr,
            Err(e) => {
                warn!("{}", e);
                continue;
            },
        };

        // send image to new host
        if let Err(e) = crate::transfer::send_image(&addr,
                &dataset, &geohash, pixel_coverage, "NAIP",
                &RAW_SOURCE, 0, &tile, timestamp) {
            warn!("failed to write image to node {}: {}", addr, e);
        }
    }

    Ok(())
}

