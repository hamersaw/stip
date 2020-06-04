use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::error::Error;
use std::path::PathBuf;

pub enum AlbumStatus {
    Closed,
    Open,
}

pub enum Geocode {
    Geohash,
    QuadTile,
}

pub struct AlbumManager {
    directory: PathBuf,
    albums: HashMap<String, Album>,
}

impl AlbumManager {
    pub fn new(directory: PathBuf) -> AlbumManager {
        // TODO - read existing albums
 
        AlbumManager {
            directory: directory,
            albums: HashMap::new(),
        }
    }

    pub fn get(&self, name: &str) -> Option<&Album> {
        self.albums.get(name)
    }

    pub fn create(&mut self, dht_key_length: Option<u8>,
            geocode: Geocode, id: &str) -> Result<(), Box<dyn Error>> {
        // check if album already exists
        if self.albums.contains_key(id) {
            return Err(
                format!("album {} already exists", id).into());
        }

        // TODO - create directory and write metadata file

        self.albums.insert(id.to_string(),
            Album {
                dht_key_length: dht_key_length,
                geocode: geocode,
                status: AlbumStatus::Open,
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
    status: AlbumStatus,
}

impl Album {
    pub fn get_metadata(&self) -> (Option<u8>, &Geocode, &AlbumStatus) {
        (self.dht_key_length, &self.geocode, &self.status)
    }
}
