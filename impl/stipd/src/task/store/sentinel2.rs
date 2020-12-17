use chrono::prelude::{DateTime, Utc};
use gdal::{Dataset, Metadata};
use swarm::prelude::Dht;
use zip::ZipArchive;

use crate::RAW_SOURCE;
use crate::album::Album;

use std::error::Error;
use std::ffi::OsStr;
use std::fs::File;
use std::io::BufReader;
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
    let mut zip_metadata_option = None;
    for filename in archive.file_names() {
        let path = PathBuf::from(&filename);

        if path.file_name() == Some(OsStr::new("MTD_MSIL1C.xml")) {
            zip_metadata_option = Some(filename);
        }
    }

    // check if we identified xml metadata file and band image files
    if zip_metadata_option == None {
        return Err("unable to find xml metadata file".into());
    }

    // open gdal metadata dataset
    let zip_metadata = zip_metadata_option.unwrap();
    let metadata_filename = format!("/vsizip/{}/{}",
        record.to_string_lossy(), zip_metadata);
    let metadata_path = PathBuf::from(&metadata_filename);
    let dataset = Dataset::open(&metadata_path)?;

    // parse metadata
    let timestamp = match dataset.metadata_item("PRODUCT_START_TIME", "") {
        Some(time) => time.parse::<DateTime<Utc>>()?.timestamp(),
        None => return Err("start time metadata not found".into()),
    };

    // populate subdatasets collection
    let metadata = match dataset.metadata_domain("SUBDATASETS") {
        Some(metadata) => metadata,
        None => return Err(format!(
            "failed to find subdatasets for '{:?}'", &record).into()),
    };

    let mut subdatasets: Vec<(&str, &str)> = Vec::new();
    let mut count = 0;
    loop {
        if count + 1 >= metadata.len() {
            break;
        }

        // parse subdataset name
        let name_fields: Vec<&str> =
            metadata[count].split("=").collect();

        // parse subdataset desc
        let description_fields: Vec<&str> =
            metadata[count+1].split("=").collect();

        subdatasets.push((name_fields[1], description_fields[1]));
        count += 2;
    }

    // process data subsets
    for (i, (name, _)) in subdatasets.iter().enumerate() {
        // open dataset
        let path = PathBuf::from(name);
        let dataset = Dataset::open(&path)?;

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
                    pixel_coverage, "Sentinel-2",
                    &RAW_SOURCE, i as u8, &tile, timestamp) {
                warn!("failed to write image to node {}: {}", addr, e);
            }
        }
    }

    Ok(())
}
