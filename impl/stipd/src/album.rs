use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::error::Error;
use std::path::PathBuf;

pub enum AlbumStatus {
    Closed,
    Open,
}

pub enum SpatialHashAlgorithm {
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

    pub fn create(&mut self, dht_hash_characters: Option<u8>,
            id: &str, spatial_hash_algorithm: SpatialHashAlgorithm)
            -> Result<(), Box<dyn Error>> {
        // check if album already exists
        if self.albums.contains_key(id) {
            return Err(
                format!("album {} already exists", id).into());
        }

        // TODO - create directory

        self.albums.insert(id.to_string(),
            Album {
                dht_hash_characters: dht_hash_characters,
                spatial_hash_algorithm: spatial_hash_algorithm,
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
    dht_hash_characters: Option<u8>,
    spatial_hash_algorithm: SpatialHashAlgorithm,
    status: AlbumStatus,
}

impl Album {
    pub fn get_metadata(&self)
            -> (Option<u8>, &SpatialHashAlgorithm, &AlbumStatus) {
        (self.dht_hash_characters,
            &self.spatial_hash_algorithm, &self.status)
    }
}
