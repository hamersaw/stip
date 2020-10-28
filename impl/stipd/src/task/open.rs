use gdal::{Dataset, Metadata};

use crate::album::Album;
use crate::task::Task;

use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub struct OpenTask {
    album: Arc<RwLock<Album>>,
}

impl OpenTask {
    pub fn new(album: Arc<RwLock<Album>>) -> OpenTask {
        {
            let album = album.read().unwrap();
            info!("initailizing open task [album={}]", album.get_id());
        }

        OpenTask {
            album: album,
        }
    }
}

#[tonic::async_trait]
impl Task<PathBuf> for OpenTask {
    fn process(&self, record: &PathBuf) -> Result<(), Box<dyn Error>> {
        let dataset = Dataset::open(&record)?;

        let cloud_coverage =
                match dataset.metadata_item("CLOUD_COVERAGE", "STIP") {
            Some(cloud_coverage) => Some(cloud_coverage.parse::<f64>()?),
            None => None,
        };
        let geocode = dataset.metadata_item("GEOCODE", "STIP")
            .ok_or("image geocode metadata not found")?;
        let pixel_coverage = dataset.metadata_item("PIXEL_COVERAGE", "STIP")
            .ok_or("image pixel coverage metadata not found")?.parse::<f64>()?;
        let platform = dataset.metadata_item("PLATFORM", "STIP")
            .ok_or("image platform metadata not found")?;
        let source = dataset.metadata_item("SOURCE", "STIP")
            .ok_or("image source metadata not found")?;
        let subdataset = dataset.metadata_item("SUBDATASET", "STIP")
            .ok_or("image subdataset metadata not found")?.parse::<u8>()?;
        let tile = dataset.metadata_item("TILE", "STIP")
            .ok_or("image tile metadata not found")?;
        let timestamp = dataset.metadata_item("TIMESTAMP", "STIP")
            .ok_or("image timestamp metadata not found")?.parse::<i64>()?;

        let mut album = self.album.write().unwrap();
        album.load(cloud_coverage, &geocode, pixel_coverage,
            &platform, &source, subdataset, &tile, timestamp)?;

        Ok(())
    }

    async fn records(&self) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        // search for paths using Album
        let album = self.album.read().unwrap();
        album.get_paths()
    }
}
