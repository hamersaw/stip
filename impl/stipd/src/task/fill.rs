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
impl Task<Vec<(Image, StFile)>> for FillTask {
    fn process(&self, record: &Vec<(Image, StFile)>)
            -> Result<(), Box<dyn Error>> {
        /*// TODO - sort records by pixel_coverage
        record.sort_by(|a, b| {
            if (a.1).1 > (b.1).1 {
                CmpOrdering::Greater
            } else if (a.1).1 < (b.1).1 {
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
                    &image.2, &image.3, files.2, &image.4)?
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
            if files.1 > max_pixel_coverage {
                max_pixel_coverage = files.1;
            }
        }

        if pixel_coverage > max_pixel_coverage {
            let image = &record[0].0;
            let file = &record[0].1;

            let mut album = self.album.write().unwrap();
            if let Err(e) = album.write(&mut dataset,
                    &image.1, pixel_coverage, &image.2,
                    &FILLED_SOURCE.to_string(),
                    file.2, &image.4, image.5) {
                warn!("failed to write filled image: {}", e);
            }
        }

        Ok(())
    }

    async fn records(&self)
            -> Result<Vec<Vec<(Image, StFile)>>, Box<dyn Error>> {
        // search for source images using Album
        let mut src_records: Vec<(Image, StFile)> = {
            let album = self.album.read().unwrap();
            let images = album.list(&self.end_timestamp,
                &self.geocode, &None, &None, &self.platform, 
                self.recurse, &None, &self.start_timestamp)?;

            let mut src_records = Vec::new();
            for (image, files) in images.into_iter() {
                for file in files.into_iter() {
                    src_records.push((image.clone(), file));
                }
            }

            src_records
        };

        // order by platform, geocode, subdataset, timestamp
        src_records.sort_by(|a, b| {
            let platform_cmp = (a.0).2.cmp(&(b.0).2);
            if platform_cmp != CmpOrdering::Equal {
                return platform_cmp;
            }

            let geocode_cmp = (a.0).1.cmp(&(b.0).1);
            if geocode_cmp != CmpOrdering::Equal {
                return geocode_cmp;
            }

            let subdataset_cmp = (a.1).2.cmp(&(b.1).2);
            if subdataset_cmp != CmpOrdering::Equal {
                return subdataset_cmp;
            }

            (a.0).5.cmp(&(b.0).5)
        });

        // initialize fill image vectors
        let mut records: Vec<Vec<(Image, StFile)>> = Vec::new();
        let mut images_buf: Vec<(Image, StFile)> = Vec::new();

        let mut platform = String::new();
        let mut geocode = String::new();
        let mut subdataset = 255;
        let mut timestamp = 0i64;
        for (image, file) in src_records.into_iter() {
            if image.2 != platform || image.1 != geocode
                    || file.2 != subdataset
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
                subdataset = file.2;
                timestamp = image.5;
            }

            images_buf.push((image, file));
        }
 
        if images_buf.len() >= 2 {
            records.push(images_buf);
        }

        // filter out vectors where full pixel coverage images exist
        let records: Vec<Vec<(Image, StFile)>> = records.into_iter()
            .filter(|x| {
                let mut valid = true;
                for (_, file) in x.iter() {
                    valid = valid && file.1 != 1f64;
                }

                valid
            }).collect();

        Ok(records)
    }
}
