use chrono::prelude::{TimeZone, Utc};
use flate2::read::GzDecoder;
use gdal::{Dataset, Driver};
use gdal::raster::GdalType;
use geocode::Geocode;
use swarm::prelude::Dht;
use tar::Archive;

use crate::RAW_SOURCE;
use crate::album::Album;

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::ffi::OsStr;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

const TMP_DIR: &str = "/tmp";

pub fn process(album: &Arc<RwLock<Album>>, dht: &Arc<Dht>,
        precision: usize, record: &PathBuf) -> Result<(), Box<dyn Error>> {
    // retrieve album metadata
    let (album_id, dht_key_length, geocode) = {
        let album = album.read().unwrap();
        (album.get_id().to_string(), album.get_dht_key_length(),
            album.get_geocode().clone())
    };
 
    // parse metadata
    let tile_path = record.with_extension("").with_extension("");
    let tile = tile_path.file_name()
        .unwrap_or(OsStr::new("")).to_string_lossy();

    let date_string = &tile[17..25];
    let year = date_string[0..4].parse::<i32>()?;
    let month = date_string[4..6].parse::<u32>()?;
    let day = date_string[6..8].parse::<u32>()?;
    let datetime = Utc.ymd(year, month, day).and_hms(0, 0, 0);

    let timestamp = datetime.timestamp();

    // process subdatasets
    let mut paths = BTreeMap::new();

    let file = File::open(record)?;
    let mut archive = Archive::new(GzDecoder::new(file));
    for entry_result in archive.entries()? {
        let mut entry = entry_result?;

        let mut path = PathBuf::from(TMP_DIR);
        path.push(entry.header().path()?);

        // decompress only files with 'TIF' extension
        if path.extension().unwrap_or(OsStr::new("")) == "TIF" {
            entry.unpack(&path)?;

            // compile datasets based on rastersize
            let dataset = Dataset::open(&path)?;
            let dimensions = dataset.raster_size();

            let path_vec = paths.entry(dimensions).or_insert(Vec::new());
            path_vec.push(path);
        }
    }

    // iterate over datasets
    for (i, (_, path_vec)) in paths.iter().enumerate() {
        // split datasets
        let datasets = split_subdatasets::<u16>(
            geocode, precision, path_vec)?;

        // processes dataset splits
        process_splits(&album_id, &datasets, &dht,
            dht_key_length, i as u8, &tile, timestamp)?;

        // delete temporary tif files
        for path in path_vec.iter() {
            std::fs::remove_file(path)?;
        }
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
                &dataset, &geocode, pixel_coverage, "Landsat8C1L1",
                &RAW_SOURCE, subdataset, &tile, timestamp) {
            warn!("failed to write image to node {}: {}", addr, e);
        }
    }

    Ok(())
}

fn split_subdatasets<T: GdalType>(geocode: Geocode,
        precision: usize, subdatasets: &Vec<PathBuf>)
        -> Result<HashMap<String, Dataset>, Box<dyn Error>> {
    let mut datasets = HashMap::new();
    let driver = Driver::get("Mem").expect("get mem driver");
    for (i, path) in subdatasets.iter().enumerate() {
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
                    min_cx, max_cx, min_cy, max_cy, epsg_code)? {
                Some(split_dataset) => split_dataset,
                None => continue,
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
