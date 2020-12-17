use gdal::Dataset;
use swarm::prelude::Dht;

use crate::{Image, StFile, RAW_SOURCE, SPLIT_SOURCE};
use crate::album::Album;
use crate::task::Task;

use std::error::Error;
use std::sync::{Arc, RwLock};

pub struct SplitTask {
    album: Arc<RwLock<Album>>,
    dht: Arc<Dht>,
    end_timestamp: Option<i64>,
    geocode: Option<String>,
    geocode_bound: Option<String>,
    platform: Option<String>,
    precision: usize,
    recurse: bool,
    start_timestamp: Option<i64>,
}

impl SplitTask {
    pub fn new(album: Arc<RwLock<Album>>, dht: Arc<Dht>,
            end_timestamp: Option<i64>, geocode: Option<String>,
            geocode_bound: Option<String>, platform: Option<String>,
            precision: usize, recurse: bool,
            start_timestamp: Option<i64>) -> SplitTask {
        {
            let album = album.read().unwrap();
            info!("initailizing split task [album={}, end_timestamp={:?}, geocode={:?}, geocode_bound={:?}, platform={:?}, precision={}, recurse={}, start_timestamp={:?}]",
                album.get_id(), end_timestamp, geocode, geocode_bound,
                platform, precision, recurse, start_timestamp);
        }

        SplitTask {
            album: album,
            dht: dht,
            end_timestamp: end_timestamp,
            geocode: geocode,
            geocode_bound: geocode_bound,
            platform: platform,
            precision: precision,
            recurse: recurse,
            start_timestamp: start_timestamp,
        }
    }
}

#[tonic::async_trait]
impl Task<(Image, Vec<StFile>)> for SplitTask {
    fn process(&self, record: &(Image, Vec<StFile>))
            -> Result<(), Box<dyn Error>> {
        let image = &record.0;

        // retrieve album metadata
        let (album_id, dht_key_length, geocode) = {
            let album = self.album.read().unwrap();
            (album.get_id().to_string(), album.get_dht_key_length(),
                album.get_geocode().clone())
        };

        for file in record.1.iter() {
            // check if path exists
            let path = {
                let album = self.album.read().unwrap();
                album.get_image_path(false, &image.1,
                    &image.2, &image.3, file.2, &image.4)?
            };

            if !path.exists() {
                return Err(format!("image path '{}' does not exist",
                    path.to_string_lossy()).into());
            }

            // open image
            let dataset = Dataset::open(&path)?;

            // compute geohash window boundaries for dataset
            let epsg_code = geocode.get_epsg_code();
            let (x_interval, y_interval) =
                geocode.get_intervals(self.precision);

            let (image_min_cx, image_max_cx, image_min_cy, image_max_cy) =
                st_image::coordinate::get_bounds(&dataset, epsg_code)?;

            let window_bounds = st_image::coordinate::get_windows(
                image_min_cx, image_max_cx, image_min_cy, image_max_cy,
                x_interval, y_interval);

            // iterate over window bounds
            for (min_cx, max_cx, min_cy, max_cy) in window_bounds {
                let split_geocode = geocode.encode(
                    (min_cx + max_cx) / 2.0,
                    (min_cy + max_cy) / 2.0, self.precision)?;

                //  skip if geocode doesn't 'start_with' base image geocode
                if !split_geocode.starts_with(&image.1) {
                    continue;
                }

                // perform dataset split
                let split_dataset = match st_image::transform::split(&dataset,
                        min_cx, max_cx, min_cy, max_cy, epsg_code)? {
                    Some(split_dataset) => split_dataset,
                    None => continue,
                };

                // if image has 0.0 coverage -> don't process
                let pixel_coverage =
                    st_image::get_coverage(&split_dataset)?;
                if pixel_coverage == 0f64 {
                    continue;
                }

                // lookup geocode in dht
                let addr = match crate::task::dht_lookup(
                        &self.dht, dht_key_length, &split_geocode) {
                    Ok(addr) => addr,
                    Err(e) => {
                        warn!("{}", e);
                        continue;
                    },
                };

                // send image to new host
                if let Err(e) = crate::transfer::send_image(&addr, &album_id,
                        &split_dataset, &split_geocode, file.1, &image.2,
                        SPLIT_SOURCE, file.2, &image.4, image.5) {
                    warn!("failed to write image to node {}: {}", addr, e);
                }
            }
        }

        Ok(())
    }

    async fn records(&self)
            -> Result<Vec<(Image, Vec<StFile>)>, Box<dyn Error>> {
        // search for images using Album
        let mut records: Vec<(Image, Vec<StFile>)> = {
            let album = self.album.read().unwrap();
            album.list(&self.end_timestamp, &self.geocode, &None, &None,
                &self.platform, self.recurse, 
                &Some(RAW_SOURCE.to_string()), &self.start_timestamp)?
        };

        // filter by geocode precision length
        records = records.into_iter().filter(|x| {
                (x.0).1.len() < self.precision as usize
            }).collect();

        // filter by result bounding geocode if necessary
        if let Some(geocode) = &self.geocode_bound {
            records = records.into_iter().filter(|(image, _)| {
                    image.1.starts_with(geocode)
                        || geocode.starts_with(&image.1)
                }).collect();
        }

        Ok(records)
    }
}
