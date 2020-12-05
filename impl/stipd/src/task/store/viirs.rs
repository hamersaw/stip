use chrono::prelude::NaiveDate;
use gdal::{Dataset, Driver, Metadata};
use gdal::raster::GdalType;
use gdal_sys::GDALDataType;
use geocode::Geocode;
use swarm::prelude::Dht;

use crate::RAW_SOURCE;
use crate::album::Album;

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub fn process(album: &Arc<RwLock<Album>>, dht: &Arc<Dht>, 
        precision: usize, record: &PathBuf) 
        -> Result<(), Box<dyn Error>> {
    // retrieve album metadata
    let (album_id, dht_key_length, geocode) = {
        let album = album.read().unwrap();
        (album.get_id().to_string(), album.get_dht_key_length(),
            album.get_geocode().clone())
    };

    let dataset = Dataset::open(&record)?;
 
    // parse metadata
    let tile_path = record.with_extension("");
    let tile = tile_path.file_name()
        .unwrap_or(OsStr::new("")).to_string_lossy();

    let start_date = match dataset.metadata_item("RangeBeginningDate", "") {
        Some(date) => NaiveDate::parse_from_str(&date, "%Y-%m-%d")?,
        None => panic!("start date metadata not found"),
    };

    let timestamp = start_date.and_hms(0, 0, 0).timestamp();

    // classify subdatasets
    let mut subdatasets = BTreeMap::new();

    let metadata = match dataset.metadata_domain("SUBDATASETS") {
        Some(metadata) => metadata,
        None => return Err(format!(
            "failed to find subdatasets for '{:?}'", &record).into()),
    };

    for i in (0..metadata.len()).step_by(2) {
        // parse subdataset name
        let name_fields: Vec<&str> =
            metadata[i].split("=").collect();

        // parse subdataset description
        let desc_fields: Vec<&str> =
            metadata[i+1].split("=").collect();

        // identify subdataset data type
        let indices: Vec<_> =
            desc_fields[1].match_indices("(").collect();

        let start_index = indices.last().unwrap_or(&(0,"")).0 + 1;
        let end_index = desc_fields[1].len()-1;

        let type_desc = &desc_fields[1][start_index..end_index];

        let data_type = match type_desc {
            "8-bit unsigned character" => GDALDataType::GDT_Byte,
            "16-bit unsigned integer" => GDALDataType::GDT_UInt16,
            "32-bit floating-point" => continue,
            _ => return Err(format!(
                "unsupported data type: '{}'", type_desc).into()),
        };

        // append to data type vector
        let vec = subdatasets.entry(data_type).or_insert(Vec::new());
        vec.push((name_fields[1], desc_fields[1]));
    }

    // process subdatasets
    for (i, (data_type, subdatasets)) in 
            subdatasets.into_iter().enumerate() {
        // split datasets
        let datasets = match data_type {
            GDALDataType::GDT_Byte => split_subdatasets::<u8>(
                geocode, precision, subdatasets)?,
            GDALDataType::GDT_UInt16 => split_subdatasets::<u16>(
                geocode, precision, subdatasets)?,
            _ => unreachable!(),
        };

        process_splits(&album_id, &datasets, &dht,
            dht_key_length, i as u8, &tile, timestamp)?;
    }

    Ok(())
}

fn process_splits(album_id: &str, datasets: &HashMap<String, Dataset>,
        dht: &Arc<Dht>, dht_key_length: i8, subdataset: u8, 
        tile: &str, timestamp: i64) -> Result<(), Box<dyn Error>> {
    for (geocode, dataset) in datasets.iter() {
        // if image has 0.0 coverage -> don't process
        let pixel_coverage = st_image::get_coverage(&dataset)?;
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
                &dataset, &geocode, pixel_coverage, "VNP21V001",
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
                    min_cx, max_cx, min_cy, max_cy, epsg_code) {
                Ok(split_dataset) => split_dataset,
                Err(e) => {
                    warn!("failed to split dataset: {}", e);
                    continue
                },
            };

            let (x, y) = split_dataset.raster_size();

            // if geocode dataset does not exist -> create it
            let split_geocode = geocode.encode((min_cx + max_cx) / 2.0,
                (min_cy + max_cy) / 2.0, precision)?;

            if !datasets.contains_key(&split_geocode) {
                let dst_dataset = driver.create_with_band_type::<T>(
                    "", x as isize, y as isize,
                    subdatasets.len() as isize)?;

                dst_dataset.set_geo_transform(
                    &split_dataset.geo_transform()?)?;
                dst_dataset.set_projection(
                    &split_dataset.projection())?;

                datasets.insert(split_geocode.clone(), dst_dataset);
            }

            let dst_dataset = datasets.get(&split_geocode).unwrap();

            // copy image raster
            st_image::copy_raster(&split_dataset, 1, (0, 0), (x, y),
                dst_dataset, (i + 1) as isize, (0, 0), (x, y))?;
        }
    }

    Ok(datasets)
}
