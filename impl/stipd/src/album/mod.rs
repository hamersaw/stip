use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::error::Error;
use std::path::PathBuf;

pub enum AlbumIndex {
    Sqlite,
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
            return Err(format!("album {} already exists", id).into());
        }

        // TODO - create directory and write metadata file

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
