use failure::ResultExt;
use gdal::raster::Dataset;

use crate::{Image, StFile, FILLED_SOURCE};
use crate::album::Album;
use crate::task::Task;

use std::cmp::Ordering as CmpOrdering;
use std::error::Error;
use std::sync::{Arc, RwLock};

pub struct FillTask {
    album: Arc<RwLock<Album>>,
    end_timestamp: Option<i64>,
    geocode: Option<String>,
    platform: Option<String>,
    recurse: bool,
    start_timestamp: Option<i64>,
    window_seconds: i64,
}

impl FillTask {
    pub fn new(album: Arc<RwLock<Album>>, end_timestamp: Option<i64>,
            geocode: Option<String>, platform: Option<String>,
            recurse: bool, start_timestamp: Option<i64>,
            window_seconds: i64) -> FillTask {
        FillTask {
            album: album,
            end_timestamp: end_timestamp,
            geocode: geocode,
            platform: platform,
            recurse: recurse,
            start_timestamp: start_timestamp,
            window_seconds: window_seconds,
        }
    }
}

#[tonic::async_trait]
impl Task<Vec<(Image, Vec<StFile>)>> for FillTask {
    fn process(&self, record: &Vec<(Image, Vec<StFile>)>)
            -> Result<(), Box<dyn Error>> {
        for i in 0..record[0].1.len() {
            // TODO - sort records by pixel_coverage
            /*record.sort_by(|a, b| {
                if a.pixel_coverage > b.pixel_coverage {
                    CmpOrdering::Greater
                } else if a.pixel_coverage > b.pixel_coverage {
                    CmpOrdering::Less
                } else {
                    CmpOrdering::Equal
                }
            });*/

            // read datasets
            let mut datasets = Vec::new();
            for (image, files) in record.iter() {
                // check if path exists
                let path = {
                    let album = self.album.read().unwrap();
                    album.get_image_path(false, &image.1,
                        &image.2, &image.3, files[i].2, &image.4)?
                };

                if !path.exists() {
                    return Err(format!("image path '{}' does not exist",
                        path.to_string_lossy()).into());
                }

                // open image
                let dataset = Dataset::open(&path).compat()?;
                datasets.push(dataset);
            }

            // perform fill
            let mut dataset = st_image::prelude::fill(&datasets)?;
            let pixel_coverage = st_image::coverage(&dataset)?;

            // check if pixel coverage is more than previous highest
            let mut max_pixel_coverage = 0f64;
            for (_, files) in record.iter() {
                if files[i].1 > max_pixel_coverage {
                    max_pixel_coverage = files[i].1;
                }
            }

            if pixel_coverage > max_pixel_coverage {
                let image = &record[0].0;
                let file = &record[0].1[i];

                let mut album = self.album.write().unwrap();
                if let Err(e) = album.write(&mut dataset,
                        &image.1, pixel_coverage, &image.2,
                        &FILLED_SOURCE.to_string(),
                        file.2, &image.4, image.5) {
                    warn!("failed to write filled image: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn records(&self)
            -> Result<Vec<Vec<(Image, Vec<StFile>)>>, Box<dyn Error>> {
        // search for source images using Album
        let mut src_records: Vec<(Image, Vec<StFile>)> = {
            let album = self.album.read().unwrap();
            album.list(&self.end_timestamp, &self.geocode,
                &None, &None, &self.platform, 
                self.recurse, &None, &self.start_timestamp)?
        };

        // order by platform, geocode, timestamp
        src_records.sort_by(|a, b| {
            let platform_cmp = (a.0).2.cmp(&(b.0).2);
            if platform_cmp != CmpOrdering::Equal {
                return platform_cmp;
            }

            let geocode_cmp = (a.0).1.cmp(&(b.0).1);
            if geocode_cmp != CmpOrdering::Equal {
                return geocode_cmp;
            }

            (a.0).5.cmp(&(b.0).5)
        });

        // initialize fill image vectors
        let mut records: Vec<Vec<(Image, Vec<StFile>)>> = Vec::new();
        let mut images_buf: Vec<(Image, Vec<StFile>)> = Vec::new();

        let mut platform = String::new();
        let mut geocode = String::new();
        let mut timestamp = 0i64;
        for (image, files) in src_records.into_iter() {
            if image.2 != platform || image.1 != geocode
                    || image.5 - timestamp > self.window_seconds {
                // process images_buf
                if images_buf.len() >= 2 {
                    records.push(images_buf);
                    images_buf = Vec::new();
                } else {
                    images_buf.clear();
                }

                // reset geocode and timestamp
                platform = image.2.clone();
                geocode = image.1.clone();
                timestamp = image.5;
            }

            images_buf.push((image, files));
        }
 
        if images_buf.len() >= 2 {
            records.push(images_buf);
        }

        // filter out vectors where full pixel coverage images exist
        let records: Vec<Vec<(Image, Vec<StFile>)>> = records.into_iter()
            .filter(|x| {
                let mut valids = vec!(true; x[0].1.len());
                for (_, files) in x.iter() {
                    for (i, file) in files.iter().enumerate() {
                        valids[i] = valids[i] && file.1 != 1f64;
                    }
                }

                let mut valid = true;
                for value in valids {
                    valid = valid && value;
                }

                valid
            }).collect();

        Ok(records)
    }
}
