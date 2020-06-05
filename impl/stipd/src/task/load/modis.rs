use chrono::prelude::{DateTime, Utc};
use failure::ResultExt;
use gdal::metadata::Metadata;
use gdal::raster::{Dataset, Driver};
use gdal::raster::types::GdalType;
use geohash::{self, Coordinate};
use swarm::prelude::Dht;

use crate::image::RAW_SOURCE;

use std::collections::HashMap;
use std::error::Error;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub fn process(album: &str, dht: &Arc<RwLock<Dht>>,
        precision: usize, record: &PathBuf, x_interval: f64,
        y_interval: f64) -> Result<(), Box<dyn Error>> {
    let dataset = Dataset::open(&record).compat()?;
 
    // parse metadata
    let tile_path = record.with_extension("");
    let tile = tile_path.file_name()
        .unwrap_or(OsStr::new("")).to_string_lossy();

    let timestamp = match dataset.metadata_item("PRODUCTIONDATETIME", "") {
        Some(time) => time.parse::<DateTime<Utc>>()?.timestamp(),
        None => return Err("start time metadata not found".into()),
    };

    // populate subdataset vectors
    let mut quality_subdatasets = Vec::new();
    let mut reflectance_subdatasets = Vec::new();

    let metadata = dataset.metadata("SUBDATASETS");
    for i in (0..metadata.len()).step_by(2) {
        // parse subdataset name
        let name_fields: Vec<&str> =
            metadata[i].split("=").collect();

        // parse subdataset description
        let desc_fields: Vec<&str> =
            metadata[i+1].split("=").collect();

        // classify subdataset as 'quality' or 'reflectance'
        if metadata[i].contains("Quality_Band") {
            quality_subdatasets.push(
                (name_fields[1], desc_fields[1]));
        } else if metadata[i].contains("Reflectance_Band") {
            reflectance_subdatasets.push(
                (name_fields[1], desc_fields[1]));
        }
    }

    // process quality subdatasets
    let quality_datasets = split_subdatasets::<u8>(
        quality_subdatasets, precision, x_interval, y_interval)?;
    process_splits(album, &quality_datasets,
        &dht, 0, &tile, timestamp)?;

    // process reflectance subdatasets
    let reflectance_datasets = split_subdatasets::<i16>(
        reflectance_subdatasets, precision, x_interval, y_interval)?;
    process_splits(album, &reflectance_datasets,
        &dht, 1, &tile, timestamp)?;

    Ok(())
}

fn process_splits(album: &str, datasets: &HashMap<String, Dataset>,
        dht: &Arc<RwLock<Dht>>, subdataset: u8, tile: &str,
        timestamp: i64) -> Result<(), Box<dyn Error>> {
    for (geohash, dataset) in datasets.iter() {
        // if image has 0.0 coverage -> don't process
        let pixel_coverage = st_image::coverage(&dataset).compat()?;
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
        if let Err(e) = crate::transfer::send_image(&addr, album,
                &dataset, &geohash, pixel_coverage, "MODIS",
                &RAW_SOURCE, subdataset, &tile, timestamp) {
            warn!("failed to write image to node {}: {}", addr, e);
        }
    }

    Ok(())
}

fn split_subdatasets<T: GdalType>(subdatasets: Vec<(&str, &str)>,
        precision: usize, x_interval: f64, y_interval: f64)
        -> Result<HashMap<String, Dataset>, Box<dyn Error>> {
    let mut datasets = HashMap::new();
    let driver = Driver::get("Mem").expect("get mem driver");
    for (i, (name, _)) in subdatasets.iter().enumerate() {
        let path = PathBuf::from(name);
        let dataset = Dataset::open(&path).expect("subdataset open");

        // split dataset
        for dataset_split in st_image::prelude::split(&dataset,
                4326, x_interval, y_interval).compat()? {
            let (_, win_max_x, _, win_max_y) =
                dataset_split.coordinates();
            let coordinate = Coordinate{x: win_max_x, y: win_max_y};
            let geohash = geohash::encode(coordinate, precision)?;

            // perform dataset split
            let dataset = dataset_split.dataset().compat()?;
            let (x, y) = dataset.size();

            // TODO - tmp print
            //println!("      {} - {:?}", geohash, dataset.size());

            // if geohash dataset does not exist -> create it
            if !datasets.contains_key(&geohash) {
                let dst_dataset = driver.create_with_band_type::<T>(
                    "", x as isize, y as isize,
                    subdatasets.len() as isize).compat()?;

                dst_dataset.set_geo_transform(
                    &dataset.geo_transform().compat()?).compat()?;
                dst_dataset.set_projection(
                    &dataset.projection()).compat()?;

                datasets.insert(geohash.clone(), dst_dataset);
            }

            let dst_dataset = datasets.get(&geohash).unwrap();

            // copy image raster
            //println!("  COPYING RASTER: {:?}", dataset.band_type(1)); 
            st_image::prelude::copy_raster(&dataset, 1, (0, 0), (x, y),
                dst_dataset, (i + 1) as isize, (0, 0), (x, y)).compat()?;
        }
    }

    Ok(datasets)
}
