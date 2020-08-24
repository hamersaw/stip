use swarm::prelude::Dht;

mod modis;
mod naip;
mod nlcd;
mod sentinel_2;

use crate::album::Album;
use crate::task::Task;

use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub enum ImageFormat {
    MODIS,
    NAIP,
    NLCD,
    Sentinel,
}

pub struct StoreEarthExplorerTask {
    album: Arc<RwLock<Album>>,
    dht: Arc<Dht>,
    format: ImageFormat,
    glob: String,
    precision: usize,
}

impl StoreEarthExplorerTask {
    pub fn new(album: Arc<RwLock<Album>>, dht: Arc<Dht>,
            format: ImageFormat, glob: String, precision: usize)
            -> StoreEarthExplorerTask {
        StoreEarthExplorerTask {
            album: album,
            dht: dht,
            format: format,
            glob: glob,
            precision: precision,
        }
    }
}

#[tonic::async_trait]
impl Task<PathBuf> for StoreEarthExplorerTask {
    fn process(&self, record: &PathBuf) -> Result<(), Box<dyn Error>> {
        match self.format {
            ImageFormat::MODIS => modis::process(
                &self.album, &self.dht, self.precision, &record),
            ImageFormat::NAIP => naip::process(
                &self.album, &self.dht, self.precision, &record),
            ImageFormat::NLCD => nlcd::process(
                &self.album, &self.dht, self.precision, &record),
            ImageFormat::Sentinel => sentinel_2::process(
                &self.album, &self.dht, self.precision, &record),
        }
    }

    async fn records(&self) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        // search for image files
        let mut records = Vec::new();
        for entry in glob::glob(&self.glob)? {
            records.push(entry?);
        }

        Ok(records)
    }
}
