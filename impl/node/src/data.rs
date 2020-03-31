use image::ImageFormat;
use st_image::StImage;

use std::error::Error;
use std::fs::File;
use std::path::PathBuf;

pub struct DataManager {
    directory: PathBuf,
}

impl DataManager {
    pub fn new(directory: PathBuf) -> DataManager {
        // TODO - iterate over .meta files and 
        //   add data to in-memory structure

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

        // TODO - add metadata to in-memory structure

        Ok(())
    }
}
