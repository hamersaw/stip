use chrono::prelude::{TimeZone, Utc};
use failure::ResultExt;
use gdal::raster::Dataset;
use swarm::prelude::Dht;

use crate::RAW_SOURCE;
use crate::album::Album;

use std::error::Error;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub fn process(album: &Arc<RwLock<Album>>, dht: &Arc<RwLock<Dht>>,
        precision: usize, record: &PathBuf) -> Result<(), Box<dyn Error>> {
    // retrieve album metadata
    let (album_id, dht_key_length, geocode) = {
        let album = album.read().unwrap();
        (album.get_id().to_string(), album.get_dht_key_length(),
            album.get_geocode().clone())
    };

    // open file
    let dataset = Dataset::open(record).compat()?;
    let filename = record.file_name().unwrap()
        .to_string_lossy().to_lowercase();

    // parse metadata
    let year = filename[filename.len()-32..filename.len()-28]
        .parse::<i32>()?;
    let datetime = Utc.ymd(year, 1, 1).and_hms(0, 0, 0);

    let timestamp = datetime.timestamp();

    let tile_path = record.with_extension("");
    let tile = tile_path.file_name()
        .unwrap_or(OsStr::new("")).to_string_lossy();

    // split image with geocode precision
    for dataset_split in st_image::prelude::split(
            &dataset, geocode, precision)? {
        // calculate split dataset geocode
        let (win_min_x, win_max_x, win_min_y, win_max_y) =
            dataset_split.coordinates();
        let split_geocode = geocode.get_code(
            (win_min_x + win_max_x) / 2.0,
            (win_min_y + win_max_y) / 2.0, precision)?;

        // perform dataset split
        let dataset = dataset_split.dataset()?;

        // if image has 0.0 coverage -> don't process
        let pixel_coverage = st_image::coverage(&dataset)?;
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
                &dataset, &split_geocode, pixel_coverage, "NLCD",
                &RAW_SOURCE, 0, &tile, timestamp) {
            warn!("failed to write image to node {}: {}", addr, e);
        }
    }

    Ok(())
}

