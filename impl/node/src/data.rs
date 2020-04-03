use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use image::ImageFormat;
use st_image::StImage;

use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

pub struct ImageMetadata {
    pub coverage: f64,
    pub geohash: String,
    pub lat_min: f64,
    pub lat_max: f64,
    pub long_min: f64,
    pub long_max: f64,
    pub path: String,
    pub platform: String,
    pub precision: usize,
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

    pub fn write_image(&self, spacecraft_id: &str, product_id: &str,
            st_image: &StImage) -> Result<(), Box<dyn Error>> {
        // create directory 'self.directory/spacecraft_id/geohash'
        let mut path = self.directory.clone();
        path.push(spacecraft_id);
        if let Some(geohash) = st_image.geohash() {
            path.push(geohash);
        }

        std::fs::create_dir_all(&path)?;

        // save image file
        path.push(product_id);
        path.set_extension("png");

        {
            let image = st_image.get_image();
            image.save_with_format(&path, ImageFormat::Png)?;
        }

        // write metadata file
        path.set_extension("meta");
        let mut metadata_file = File::create(&path)?;

        st_image.write_metadata(&mut metadata_file)?;

        // write image 'coverage'
        match st_image.coverage() {
            Some(coverage) => {
                metadata_file.write_u8(1)?;
                metadata_file.write_f64::<BigEndian>(coverage)?;
            },
            None => metadata_file.write_u8(0)?,
        }

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

            // read StImage metadata from file
            let mut file = File::open(&path)?;
            let (lat_min, lat_max, long_min, long_max, precision) =
                StImage::read_metadata(&mut file)?;

            // read 'coverage'
            let coverage = match file.read_u8()? {
                0 => -1.0,
                _ => file.read_f64::<BigEndian>()?,
            };
 
            // parse platform and geohash from path
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
                lat_min: lat_min,
                lat_max: lat_max,
                long_min: long_min,
                long_max: long_max,
                path: path.to_string_lossy().to_string(),
                platform: platform,
                precision: precision.unwrap_or(0),
            };

            vec.push(image_metadata);
        }

        Ok(vec)
    }
}
