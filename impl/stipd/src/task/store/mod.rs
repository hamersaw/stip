use swarm::prelude::Dht;

mod gridmet;
mod modis;
mod naip;
mod nlcd;
mod sentinel2;
mod viirs;

use crate::album::Album;
use crate::task::Task;

use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub enum ImageFormat {
    GridMET,
    MCD43A4,
    MOD11A1,
    MOD11A2,
    NAIP,
    NLCD,
    Sentinel2,
    VNP21V001,
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
        {
            let album = album.read().unwrap();
            info!("initailizing store task [album={}, format={:?}, glob={}, precision={}]",
                album.get_id(), format, glob, precision)
        }
            
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
            ImageFormat::GridMET => gridmet::process(
                &self.album, &self.dht, self.precision, &record),
            ImageFormat::MCD43A4 => modis::process(&self.album,
                "MCD43A4", &self.dht, self.precision, &record),
            ImageFormat::MOD11A1 => modis::process(&self.album,
                "MOD11A1", &self.dht, self.precision, &record),
            ImageFormat::MOD11A2 => modis::process(&self.album,
                "MOD11A2", &self.dht, self.precision, &record),
            ImageFormat::NAIP => naip::process(
                &self.album, &self.dht, self.precision, &record),
            ImageFormat::NLCD => nlcd::process(
                &self.album, &self.dht, self.precision, &record),
            ImageFormat::Sentinel2 => sentinel2::process(
                &self.album, &self.dht, self.precision, &record),
            ImageFormat::VNP21V001 => viirs::process(
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
