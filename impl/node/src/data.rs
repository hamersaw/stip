use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::raster::{Dataset, Driver};

use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

pub struct ImageMetadata {
    pub coverage: f64,
    pub geohash: String,
    pub path: String,
    pub platform: String,
}

pub struct DataManager {
    directory: PathBuf,
}

impl DataManager {
    pub fn new(directory: PathBuf) -> DataManager {
        DataManager {
            directory: directory,
        }
    }

    pub fn write_image(&self, platform: &str, geohash: &str, tile: &str,
            dataset: &Dataset) -> Result<(), Box<dyn Error>> {
        // create directory 'self.directory/platform/geohash'
        let mut path = self.directory.clone();
        path.push(platform);
        path.push(geohash);

        std::fs::create_dir_all(&path)?;

        // save image file 'self.directory/platform/geohash/tile' - TODO error
        path.push(tile);
        path.set_extension("tif");
        
        let driver = Driver::get("GTiff").unwrap();
        dataset.create_copy(&driver, &path.to_string_lossy()).unwrap();

        // write metadata file
        path.set_extension("meta");
        let mut metadata_file = File::create(&path)?;

        // write image 'coverage' - TODO error
        let coverage = st_image::coverage(&dataset).unwrap();
        metadata_file.write_f64::<BigEndian>(coverage)?;

        Ok(())
    }

    pub fn search_images(&self, geohash: &str, platform: &str)
            -> Result<Vec<ImageMetadata>, Box<dyn Error>> {
        // compile glob file search regex
        let directory = format!("{}/{}/{}/*meta",
            self.directory.to_string_lossy(), platform, geohash);

        // search for metadata files
        let mut vec = Vec::new();
        for entry in glob::glob(&directory)? {
            let mut path = entry?;
            let mut file = File::open(&path)?;

            // read 'coverage'
            let coverage = file.read_f64::<BigEndian>()?;

            // parse platform and geohash from path
            path.set_extension("tif");
            let path_str = path.to_string_lossy().to_string();
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
                coverage: coverage,
                geohash: geohash,
                path: path_str,
                platform: platform,
            };

            vec.push(image_metadata);
        }

        Ok(vec)
    }
}
