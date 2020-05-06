use gdal::metadata::Metadata;
use gdal::raster::{Dataset, Driver};

use std::error::Error;
use std::ffi::CString;
use std::path::PathBuf;

pub const FILLED_SOURCE: &'static str = "filled";
pub const RAW_SOURCE: &'static str = "raw";

#[derive(Clone, Debug)]
pub struct ImageMetadata {
    pub band: String,
    pub cloud_coverage: Option<f32>,
    pub end_date: i64,
    pub geohash: String,
    pub path: String,
    pub pixel_coverage: f32,
    pub platform: String,
    pub source: String,
    pub start_date: i64,
}

pub struct ImageManager {
    directory: PathBuf,
    images: Vec<ImageMetadata>,
}

impl ImageManager {
    pub fn new(directory: PathBuf) -> ImageManager {
        ImageManager {
            directory: directory,
            images: Vec::new(),
        }
    }
    
    pub fn init(&mut self) -> Result<(), Box<dyn Error>> {
        let glob_expression = format!("{}/*/*/*/*/*tif",
            self.directory.to_string_lossy());

        // iterate over existing images
        for entry in glob::glob(&glob_expression)? {
            let mut path = entry?;
            let dataset = Dataset::open(&path).unwrap();

            // TODO - error
            let start_date = dataset.metadata_item("START_DATE",
                "STIP") .unwrap().parse::<i64>()?;
            let end_date = dataset.metadata_item("END_DATE",
                "STIP").unwrap().parse::<i64>()?;
            let pixel_coverage = dataset.metadata_item("PIXEL_COVERAGE",
                "STIP").unwrap().parse::<f32>()?;
            let cloud_coverage_dec = dataset.metadata_item("CLOUD_COVERAGE",
                "STIP").unwrap().parse::<f32>()?;

            let cloud_coverage = if cloud_coverage_dec == std::f32::MAX {
                None
            } else {
                Some(cloud_coverage_dec)
            };

            // parse platform and geohash from path
            let path_str = path.to_string_lossy().to_string();
            let _ = path.pop();
            let source = path.file_name()
                .ok_or("source not found in path")?
                .to_string_lossy().to_string();
            let _ = path.pop();
            let band = path.file_name()
                .ok_or("band not found in path")?
                .to_string_lossy().to_string();
            let _ = path.pop();
            let geohash = path.file_name()
                .ok_or("geohash not found in path")?
                .to_string_lossy().to_string();
            let _ = path.pop();
            let platform = path.file_name()
                .ok_or("platform not found in path")?
                .to_string_lossy().to_string();

            // initialize ImageMetadata
            let image_metadata = ImageMetadata {
                band: band,
                cloud_coverage: cloud_coverage,
                end_date: end_date,
                geohash: geohash,
                path: path_str,
                pixel_coverage: pixel_coverage,
                platform: platform,
                source: source,
                start_date: start_date,
            };

            self.images.push(image_metadata);
        }
        
        Ok(())
    }

    pub fn write(&mut self, platform: &str, geohash: &str, band: &str, 
            source: &str, tile: &str, start_date: i64, 
            end_date: i64, pixel_coverage: f32, dataset: &mut Dataset)
            -> Result<(), Box<dyn Error>> {
        // create directory 'self.directory/platform/geohash/band/dataset'
        let mut path = self.directory.clone();
        path.push(platform);
        path.push(geohash);
        path.push(band);
        path.push(source);

        std::fs::create_dir_all(&path)?;

        path.push(tile);
        path.set_extension("tif");

        // open GeoTiff driver
        let driver = Driver::get("GTiff").unwrap();

        // copy image to GeoTiff format
        let mut c_options = vec![
            CString::new("COMPRESS=LZW")?.into_raw(),
            std::ptr::null_mut()
        ];

        // TODO error
        let path_str = path.to_string_lossy();
        let mut dataset_copy = dataset.create_copy(&driver,
            &path_str, Some(c_options.as_mut_ptr())).unwrap();

        // clean up potential memory leaks
        unsafe {
            for ptr in c_options {
                if !ptr.is_null() {
                    let _ = CString::from_raw(ptr);
                }
            }
        }

        // set dataset metadata attributes - TODO error
        dataset_copy.set_metadata_item("START_DATE",
            &start_date.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("END_DATE",
            &end_date.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("PIXEL_COVERAGE",
            &pixel_coverage.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("CLOUD_COVERAGE",
            &format!("{}", std::f32::MAX), "STIP").unwrap();

        // TODO - add image to self.images
        self.images.push(
            ImageMetadata {
                band: band.to_string(),
                cloud_coverage: None,
                end_date: end_date,
                geohash: geohash.to_string(),
                path: path_str.to_string(),
                pixel_coverage: pixel_coverage,
                platform: platform.to_string(),
                source: source.to_string(),
                start_date: start_date,
            });

        Ok(())
    }

    pub fn search(&self, band: &Option<String>,
            geohash: &Option<String>, platform: &Option<String>,
            recurse: bool, source: &Option<String>)
            -> Vec<&ImageMetadata> {
        let mut images: Vec<&ImageMetadata> =
            self.images.iter().collect();

        // if exists - filter on band
        if let Some(band) = band {
            images = images.into_iter()
                .filter(|x| &x.band == band).collect();
        }

        // if exists - filter on geohash
        if let Some(geohash) = geohash {
            images = match recurse {
                true => images.into_iter()
                    .filter(|x| x.geohash.starts_with(geohash)).collect(),
                false => images.into_iter()
                    .filter(|x| &x.geohash == geohash).collect(),
            };
        }

        // if exists - filter on platform
        if let Some(platform) = platform {
            images = images.into_iter()
                .filter(|x| &x.platform == platform).collect();
        }

        // if exists - filter on source
        if let Some(source) = source {
            images = images.into_iter()
                .filter(|x| &x.source == source).collect();
        }

        images
    }

    //pub fn search(&self, band: &str, geohash: &str,
    //        platform: &str, recurse: bool, source: &str)
    //        -> Result<Vec<ImageMetadata>, Box<dyn Error>> {
        // compile glob file search regex
        //let recurse_geohash = match recurse {
        //    true => format!("{}*", geohash),
        //    false => geohash.to_string(),
        //};
        
        //let directory = format!("{}/{}/{}/{}/{}/*tif",
        //    self.directory.to_string_lossy(), platform,
        //    recurse_geohash, band, source);

        // TODO - search for metadata files
    //    let mut vec = Vec::new();

    //    Ok(vec)
    //}
}
