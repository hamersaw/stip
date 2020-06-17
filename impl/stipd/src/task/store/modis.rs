use chrono::prelude::{DateTime, Utc};
use failure::ResultExt;
use gdal::metadata::Metadata;
use gdal::raster::{Dataset, Driver};
use gdal::raster::types::GdalType;
use st_image::prelude::Geocode;
use swarm::prelude::Dht;

use crate::RAW_SOURCE;
use crate::album::Album;

use std::collections::HashMap;
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
    let quality_datasets = split_subdatasets::<u8>(geocode,
        precision, quality_subdatasets)?;
    process_splits(&album_id, &quality_datasets,
        &dht, dht_key_length, 0, &tile, timestamp)?;

    // process reflectance subdatasets
    let reflectance_datasets = split_subdatasets::<i16>(geocode,
        precision, reflectance_subdatasets)?;
    process_splits(&album_id, &reflectance_datasets,
        &dht, dht_key_length, 1, &tile, timestamp)?;

    Ok(())
}

fn process_splits(album_id: &str, datasets: &HashMap<String, Dataset>,
        dht: &Arc<RwLock<Dht>>, dht_key_length: i8, subdataset: u8, 
        tile: &str, timestamp: i64) -> Result<(), Box<dyn Error>> {
    for (geocode, dataset) in datasets.iter() {
        // if image has 0.0 coverage -> don't process
        let pixel_coverage = st_image::coverage(&dataset)?;
        if pixel_coverage == 0f64 {
            continue;
        }

        // lookup geocode in dht
        let addr = match crate::task::dht_lookup(
                &dht, dht_key_length, &geocode) {
            Ok(addr) => addr,
            Err(e) => {
                warn!("{}", e);
                continue;
            },
        };

        // send image to new host
        if let Err(e) = crate::transfer::send_image(&addr, album_id,
                &dataset, &geocode, pixel_coverage, "MODIS",
                &RAW_SOURCE, subdataset, &tile, timestamp) {
            warn!("failed to write image to node {}: {}", addr, e);
        }
    }

    Ok(())
}

fn split_subdatasets<T: GdalType>(geocode: Geocode,
        precision: usize, subdatasets: Vec<(&str, &str)>)
        -> Result<HashMap<String, Dataset>, Box<dyn Error>> {
    let mut datasets = HashMap::new();
    let driver = Driver::get("Mem").expect("get mem driver");
    for (i, (name, _)) in subdatasets.iter().enumerate() {
        let path = PathBuf::from(name);
        let dataset = Dataset::open(&path).expect("subdataset open");

        // split dataset
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
            let (x, y) = dataset.size();

            // if geocode dataset does not exist -> create it
            if !datasets.contains_key(&split_geocode) {
                let dst_dataset = driver.create_with_band_type::<T>(
                    "", x as isize, y as isize,
                    subdatasets.len() as isize).compat()?;

                dst_dataset.set_geo_transform(
                    &dataset.geo_transform().compat()?).compat()?;
                dst_dataset.set_projection(
                    &dataset.projection()).compat()?;

                datasets.insert(split_geocode.clone(), dst_dataset);
            }

            let dst_dataset = datasets.get(&split_geocode).unwrap();

            // copy image raster
            //println!("  COPYING RASTER: {:?}", dataset.band_type(1)); 
            st_image::prelude::copy_raster(&dataset, 1, (0, 0), (x, y),
                dst_dataset, (i + 1) as isize, (0, 0), (x, y))?;
        }
    }

    Ok(datasets)
}
