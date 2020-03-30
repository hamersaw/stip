use st_image::StImage;

use std::error::Error;
use std::path::PathBuf;

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
        unimplemented!();
    }
}
