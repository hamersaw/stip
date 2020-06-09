use byteorder::{ReadBytesExt, WriteBytesExt};
use failure::ResultExt;
use gdal::metadata::Metadata;
use gdal::raster::{Dataset, Driver};
use st_image::prelude::Geocode;

use crate::{Extent, Image, StFile};
use crate::index::AlbumIndex;

use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::error::Error;
use std::ffi::CString;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::os::unix::fs::PermissionsExt;

pub struct AlbumManager {
    directory: PathBuf,
    albums: HashMap<String, Arc<RwLock<Album>>>,
}

impl AlbumManager {
    pub fn new(directory: PathBuf)
            -> Result<AlbumManager, Box<dyn Error>> {
        // parse existing albums
        let mut albums = HashMap::new();
        for entry in std::fs::read_dir(&directory)? {
            let mut path = entry?.path();
            let id = path.file_name().unwrap()
                .to_string_lossy().to_string();

            // parse metadata file
            path.push("album");
            path.set_extension("meta");
            let mut file = File::open(&path)?;

            let dht_key_length = file.read_i8()?;
            let geocode_value = file.read_u8()?;
            let geocode: Geocode = match geocode_value {
                0 => Geocode::Geohash,
                1 => Geocode::QuadTile,
                _ => return Err(format!("unknown geocode {}",
                    geocode_value).into()),
            };

            path.pop();

            // add album to map
            albums.insert(id.clone(),
                Arc::new(RwLock::new(Album {
                    dht_key_length: dht_key_length,
                    directory: path,
                    geocode: geocode,
                    id: id,
                    index: None,
                })));
        }

        Ok(AlbumManager {
            directory: directory,
            albums: albums,
        })
    }

    pub fn get(&self, name: &str) -> Option<&Arc<RwLock<Album>>> {
        self.albums.get(name)
    }

    pub fn create(&mut self, dht_key_length: i8, geocode: Geocode,
            id: &str) -> Result<(), Box<dyn Error>> {
        // create album directory
        let mut path = self.directory.clone();
        path.push(id);

        std::fs::create_dir(&path)?;
        let mut permissions = std::fs::metadata(&path)?.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions)?;

        // write metadata file
        path.push("album");
        path.set_extension("meta");
        let mut file = File::create(&path)?;

        file.write_i8(dht_key_length)?;
        match geocode {
            Geocode::Geohash => file.write_u8(0)?,
            Geocode::QuadTile => file.write_u8(1)?,
        }
        path.pop();

        // add album to map
        self.albums.insert(id.to_string(),
            Arc::new(RwLock::new(Album {
                dht_key_length: dht_key_length,
                directory: path,
                geocode: geocode,
                id: id.to_string(),
                index: None,
            })));

        Ok(())
    }

    pub fn iter(&self) -> Iter<String, Arc<RwLock<Album>>> {
        self.albums.iter()
    }
}

pub struct Album {
    dht_key_length: i8,
    directory: PathBuf,
    geocode: Geocode,
    id: String,
    index: Option<AlbumIndex>,
}

impl Album {
    pub fn close(&mut self) {
        self.index = None;
    }

    pub fn get_dht_key_length(&self) -> i8 {
        self.dht_key_length
    }

    pub fn get_geocode(&self) -> &Geocode {
        &self.geocode
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn get_image_path(&self, create: bool, geocode: &str,
            platform: &str, source: &str, subdataset: u8,
            tile: &str) -> Result<PathBuf, Box<dyn Error>> {
        // create directory 'self.directory/platform/geocode/source'
        let mut path = self.directory.clone();
        for filename in vec!(platform, geocode, source) {
            path.push(filename);
            if create && !path.exists() {
                std::fs::create_dir(&path)?;
                let mut permissions =
                    std::fs::metadata(&path)?.permissions();
                permissions.set_mode(0o755);
                std::fs::set_permissions(&path, permissions)?;
            }
        }

        // add tile-subdataset.tif
        path.push(format!("{}-{}.tif", tile, subdataset));
        Ok(path)
    }

    pub fn get_index(&self) -> &Option<AlbumIndex> {
        &self.index
    }

    pub fn get_paths(&self) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        let glob_expression = format!("{}/*/*/*/*tif",
            self.directory.to_string_lossy());

        // iterate over existing images
        let mut paths = Vec::new();
        for entry in glob::glob(&glob_expression)? {
            paths.push(entry?);
        }

        Ok(paths)
    }

    pub fn list(&self, end_timestamp: &Option<i64>,
            geocode: &Option<String>, max_cloud_coverage: &Option<f64>,
            min_pixel_coverage: &Option<f64>, platform: &Option<String>,
            recurse: bool, source: &Option<String>,
            start_timestamp: &Option<i64>)
            -> Result<Vec<(Image, Vec<StFile>)>, Box<dyn Error>> {
        match &self.index {
            Some(index) => Ok(index.list(&self, end_timestamp, geocode,
                max_cloud_coverage, min_pixel_coverage, platform,
                recurse, source, start_timestamp)?),
            None => Err("unable to search on closed album".into()),
        }
    }

    pub fn load(&mut self, cloud_coverage: Option<f64>, geocode: &str,
            pixel_coverage: f64, platform: &str, source: &str,
            subdataset: u8, tile: &str, timestamp: i64) 
            -> Result<(), Box<dyn Error>> {
        match &mut self.index {
            Some(index) => Ok(index.load(cloud_coverage,
                geocode, pixel_coverage, platform, source,
                subdataset, tile, timestamp)?),
            None => Err("unable to search on closed album".into()),
        }
    }

    pub fn open(&mut self) -> Result<(), Box<dyn Error>> {
        self.index = Some(AlbumIndex::new()?);
        Ok(())
    }

    pub fn search(&self, end_timestamp: &Option<i64>,
            geocode: &Option<String>, max_cloud_coverage: &Option<f64>,
            min_pixel_coverage: &Option<f64>, platform: &Option<String>,
            recurse: bool, source: &Option<String>,
            start_timestamp: &Option<i64>)
            -> Result<Vec<Extent>, Box<dyn Error>> {
        match &self.index {
            Some(index) => Ok(index.search(end_timestamp, geocode,
                max_cloud_coverage, min_pixel_coverage, platform,
                recurse, source, start_timestamp)?),
            None => Err("unable to search on closed album".into()),
        }
    }

    pub fn write(&mut self, dataset: &mut Dataset, geocode: &str,
            pixel_coverage: f64, platform: &str, source: &str,
            subdataset: u8, tile: &str, timestamp: i64)
            -> Result<(), Box<dyn Error>> {
        // get image path
        let path = self.get_image_path(true, geocode,
            platform, source, subdataset, tile)?;

        if path.exists() { // attempting to rewrite existing file
            return Ok(());
        }

        // open GeoTiff driver
        let driver = Driver::get("GTiff").compat()?;

        // copy image to GeoTiff format
        let mut c_options = vec![
            CString::new("COMPRESS=LZW")?.into_raw(),
            std::ptr::null_mut()
        ];

        let path_str = path.to_string_lossy();
        let mut dataset_copy = dataset.create_copy(&driver,
            &path_str, Some(c_options.as_mut_ptr())).compat()?;

        // clean up potential memory leaks
        unsafe {
            for ptr in c_options {
                if !ptr.is_null() {
                    let _ = CString::from_raw(ptr);
                }
            }
        }

        // set image permissions
        let mut permissions = std::fs::metadata(&path)?.permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&path, permissions)?;

        // set dataset metadata attributes
        dataset_copy.set_metadata_item("GEOCODE",
            geocode, "STIP").compat()?;
        dataset_copy.set_metadata_item("PIXEL_COVERAGE",
            &pixel_coverage.to_string(), "STIP").compat()?;
        dataset_copy.set_metadata_item("PLATFORM",
            platform, "STIP").compat()?;
        dataset_copy.set_metadata_item("SOURCE",
            source, "STIP").compat()?;
        dataset_copy.set_metadata_item("SUBDATASET",
            &subdataset.to_string(), "STIP").compat()?;
        dataset_copy.set_metadata_item("TILE", tile, "STIP").compat()?;
        dataset_copy.set_metadata_item("TIMESTAMP",
            &timestamp.to_string(), "STIP").compat()?;

        // if album is open -> load data
        if let Some(_) = self.index {
            self.load(None, geocode, pixel_coverage,
                platform, source, subdataset, tile, timestamp)?;
        }

        Ok(())
    }
}
