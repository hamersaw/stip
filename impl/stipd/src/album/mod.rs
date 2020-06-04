use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::error::Error;
use std::fs::File;
use std::path::PathBuf;
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
    albums: HashMap<String, Album>,
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

            // add album to map
            albums.insert(id,
                Album {
                    dht_key_length: dht_key_length,
                    geocode: geocode,
                    index: None,
                });
        }

        Ok(AlbumManager {
            directory: directory,
            albums: albums,
        })
    }

    pub fn get(&self, name: &str) -> Option<&Album> {
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

        // add album to map
        self.albums.insert(id.to_string(),
            Album {
                dht_key_length: dht_key_length,
                geocode: geocode,
                index: None,
            });

        Ok(())
    }

    pub fn iter(&self) -> Iter<String, Album> {
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

    pub fn get_index(&self) -> &Option<AlbumIndex> {
        &self.index
    }
}
