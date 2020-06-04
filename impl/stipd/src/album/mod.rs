use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::metadata::Metadata;
use gdal::raster::{Dataset, Driver};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::error::Error;
use std::ffi::CString;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::os::unix::fs::PermissionsExt;

pub enum AlbumIndex {
    Sqlite,
}

#[derive(Clone, Copy, FromPrimitive)]
pub enum Geocode {
    Geohash = 1,
    QuadTile = 2,
}

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

            let dht_key_length = match file.read_u8()? {
                0 => None,
                x => Some(x),
            };

            let geocode_value = file.read_u8()?;
            let geocode: Geocode =
                    match FromPrimitive::from_u8(geocode_value) {
                Some(x) => x,
                None => return Err(format!("unknown geocode {}",
                    geocode_value).into()),
            };

            path.pop();

            // add album to map
            albums.insert(id,
                Arc::new(RwLock::new(Album {
                    dht_key_length: dht_key_length,
                    directory: path,
                    geocode: geocode,
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

    pub fn create(&mut self, dht_key_length: Option<u8>,
            geocode: Geocode, id: &str) -> Result<(), Box<dyn Error>> {
        // check if album already exists
        if self.albums.contains_key(id) {
            return Err(format!("album {} already exists", id).into());
        }

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

        match dht_key_length {
            Some(dht_key_length) => file.write_u8(dht_key_length)?,
            None => file.write_u8(0)?,
        };

        file.write_u8(geocode as u8)?;
        path.pop();

        // add album to map
        self.albums.insert(id.to_string(),
            Arc::new(RwLock::new(Album {
                dht_key_length: dht_key_length,
                directory: path,
                geocode: geocode,
                index: None,
            })));

        Ok(())
    }

    pub fn iter(&self) -> Iter<String, Arc<RwLock<Album>>> {
        self.albums.iter()
    }

    pub fn remove(&mut self, name: &str) -> Result<(), Box<dyn Error>> {
        // remove 'name' from self.albums
        self.albums.remove(name);
        Ok(())
    }
}

pub struct Album {
    dht_key_length: Option<u8>,
    directory: PathBuf,
    geocode: Geocode,
    index: Option<AlbumIndex>,
}

impl Album {
    pub fn get_dht_key_length(&self) -> Option<u8> {
        self.dht_key_length
    }

    pub fn get_geocode(&self) -> &Geocode {
        &self.geocode
    }

    pub fn get_image_path(&self, create: bool, geohash: &str,
            platform: &str, source: &str, subdataset: u8,
            tile: &str) -> Result<PathBuf, Box<dyn Error>> {
        // create directory 'self.directory/platform/geohash/source'
        let mut path = self.directory.clone();
        for filename in vec!(platform, geohash, source) {
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

    pub fn write(&mut self, dataset: &mut Dataset, geohash: &str,
            pixel_coverage: f64, platform: &str, source: &str,
            subdataset: u8, tile: &str, timestamp: i64)
            -> Result<(), Box<dyn Error>> {
        // get image path
        let path = self.get_image_path(true, geohash,
            platform, source, subdataset, tile)?;

        if path.exists() { // attempting to rewrite existing file
            return Ok(());
        }

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

        // set image permissions
        let mut permissions = std::fs::metadata(&path)?.permissions();
        permissions.set_mode(0o644);
        std::fs::set_permissions(&path, permissions)?;

        // set dataset metadata attributes - TODO error
        dataset_copy.set_metadata_item("GEOHASH",
            geohash, "STIP").unwrap();
        dataset_copy.set_metadata_item("PIXEL_COVERAGE",
            &pixel_coverage.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("PLATFORM",
            platform, "STIP").unwrap();
        dataset_copy.set_metadata_item("SOURCE",
            source, "STIP").unwrap();
        dataset_copy.set_metadata_item("SUBDATASET",
            &subdataset.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("TILE", tile, "STIP").unwrap();
        dataset_copy.set_metadata_item("TIMESTAMP",
            &timestamp.to_string(), "STIP").unwrap();

        // TODO - load data in index
        //self.load(None, geohash, pixel_coverage,
        //    platform, source, subdataset, tile, timestamp)?;

        Ok(())
    }
}
