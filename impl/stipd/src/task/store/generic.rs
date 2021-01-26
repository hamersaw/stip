use chrono::prelude::NaiveDate;
use gdal::{Dataset, Metadata};
use swarm::prelude::Dht;

use crate::RAW_SOURCE;
use crate::album::Album;

use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub fn process(album: &Arc<RwLock<Album>>, dht: &Arc<Dht>,
        precision: usize, record: &PathBuf) -> Result<(), Box<dyn Error>> {
    // retrieve album metadata
    let (album_id, dht_key_length, geocode) = {
        let album = album.read().unwrap();
        (album.get_id().to_string(), album.get_dht_key_length(),
            album.get_geocode().clone())
    };

    let dataset = Dataset::open(&record)?;

    // parse metadata
    let platform = match dataset.metadata_item("PLATFORM", "") {
        Some(platform) => platform,
        None => return Err("platform metadata not found".into()),
    };

    let subdataset = match dataset.metadata_item("SUBDATASET", "") {
        Some(subdataset) => subdataset.parse::<u8>()?,
        None => return Err("subdataset metadata not found".into()),
    };

    let tile = match dataset.metadata_item("TILE", "") {
        Some(tile) => tile,
        None => return Err("tile metadata not found".into()),
    };

    let start_date = match dataset.metadata_item("TIMESTAMP", "") {
        Some(date) => NaiveDate::parse_from_str(&date, "%Y%m%d")?,
        None => return Err("start time metadata not found".into()),
    };

    let timestamp = start_date.and_hms(0, 0, 0).timestamp();

    // compute geohash window boundaries for dataset
    let epsg_code = geocode.get_epsg_code();
    let (x_interval, y_interval) = geocode.get_intervals(precision);

    let (image_min_cx, image_max_cx, image_min_cy, image_max_cy) =
        st_image::coordinate::get_bounds(&dataset, epsg_code)?;

    let window_bounds = st_image::coordinate::get_windows(
        image_min_cx, image_max_cx, image_min_cy, image_max_cy,
        x_interval, y_interval);

    // iterate over window bounds
    for (min_cx, max_cx, min_cy, max_cy) in window_bounds {
        // perform dataset split
        let split_dataset = match st_image::transform::split(&dataset,
                min_cx, max_cx, min_cy, max_cy, epsg_code)? {
            Some(split_dataset) => split_dataset,
            None => continue,
        };

        let split_geocode = geocode.encode((min_cx + max_cx) / 2.0,
            (min_cy + max_cy) / 2.0, precision)?;

        // if image has 0.0 coverage -> don't process
        let pixel_coverage = 
            st_image::get_coverage(&split_dataset)?;
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
        if let Err(e) = crate::transfer::send_image(&addr,
                &album_id, &split_dataset, &split_geocode,
                pixel_coverage, &platform,
                &RAW_SOURCE, subdataset, &tile, timestamp) {
            warn!("failed to write image to node {}: {}", addr, e);
        }
    }

    Ok(())
}
