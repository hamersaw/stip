use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::raster::{Dataset, Driver};

use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

pub const BASE_DATASET: &'static str = "base";
pub const FILL_DATASET: &'static str = "fill";

#[derive(Clone, Debug)]
pub struct ImageMetadata {
    pub coverage: f64,
    pub dataset: String,
    pub end_date: i64,
    pub geohash: String,
    pub path: String,
    pub platform: String,
    pub start_date: i64,
}

pub struct ImageManager {
    directory: PathBuf,
}

impl ImageManager {
    pub fn new(directory: PathBuf) -> ImageManager {
        ImageManager {
            directory: directory,
        }
    }

    pub fn write(&self, platform: &str, dataset: &str, geohash: &str,
            tile: &str, start_date: i64, end_date: i64, coverage: f64,
            image: &Dataset) -> Result<(), Box<dyn Error>> {
        // create directory 'self.directory/platform/geohash'
        let mut path = self.directory.clone();
        path.push(platform);
        path.push(dataset);
        path.push(geohash);

        std::fs::create_dir_all(&path)?;

        // save image file 'self.directory/platform/geohash/tile' - TODO error
        path.push(tile);
        path.set_extension("tif");
        
        let driver = Driver::get("GTiff").unwrap();
        image.create_copy(&driver, &path.to_string_lossy()).unwrap();

        // write metadata file
        path.set_extension("meta");
        let mut metadata_file = File::create(&path)?;

        metadata_file.write_i64::<BigEndian>(start_date)?;
        metadata_file.write_i64::<BigEndian>(end_date)?;
        metadata_file.write_f64::<BigEndian>(coverage)?;

        Ok(())
    }

    pub fn search(&self, dataset: &str, geohash: &str, platform: &str)
            -> Result<Vec<ImageMetadata>, Box<dyn Error>> {
        // compile glob file search regex
        let directory = format!("{}/{}/{}/{}/*meta",
            self.directory.to_string_lossy(), platform, dataset, geohash);

        // search for metadata files
        let mut vec = Vec::new();
        for entry in glob::glob(&directory)? {
            let mut path = entry?;
            let mut file = File::open(&path)?;

            // read metadata from file
            let start_date = file.read_i64::<BigEndian>()?;
            let end_date = file.read_i64::<BigEndian>()?;
            let coverage = file.read_f64::<BigEndian>()?;

            // parse platform and geohash from path
            path.set_extension("tif");
            let path_str = path.to_string_lossy().to_string();
            let _ = path.pop();
            let geohash = path.file_name()
                .ok_or("geohash not found in path")?
                .to_string_lossy().to_string();
            let _ = path.pop();
            let dataset = path.file_name()
                .ok_or("dataset not found in path")?
                .to_string_lossy().to_string();
            let _ = path.pop();
            let platform = path.file_name()
                .ok_or("platform not found in path")?
                .to_string_lossy().to_string();

            // initialize ImageMetadata
            let image_metadata = ImageMetadata {
                coverage: coverage,
                dataset: dataset,
                end_date: end_date,
                geohash: geohash,
                path: path_str,
                platform: platform,
                start_date: start_date,
            };

            vec.push(image_metadata);
        }

        Ok(vec)
    }
}
