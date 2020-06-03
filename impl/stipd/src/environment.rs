use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;

pub enum EnvironmentStatus {
    Available,
    Loaded,
}

pub struct EnvironmentManager {
    directory: PathBuf,
    environments: HashMap<String, Environment>,
}

impl EnvironmentManager {
    pub fn new(directory: PathBuf) -> EnvironmentManager {
        // TODO - read existing environments
 
        EnvironmentManager {
            directory: directory,
            environments: HashMap::new(),
        }
    }

    pub fn initialize(&mut self, dht_hash_characters: Option<u8>,
            hash_bits: u8, id: &str, image_projection: u16)
            -> Result<(), Box<dyn Error>> {
        // check if environment already exists
        if self.environments.contains_key(id) {
            return Err(
                format!("environment {} already exists", id).into());
        }

        // TODO - create directory

        self.environments.insert(id.to_string(),
            Environment {
                dht_hash_characters: dht_hash_characters,
                hash_bits: hash_bits,
                image_projection: image_projection,
                status: EnvironmentStatus::Loaded,
            });

        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&Environment> {
        self.environments.get(name)
    }

    pub fn list(&self) -> Vec<(&String, &Environment)> {
        self.environments.iter().collect()
    }

    pub fn remove(&mut self, name: &str) -> Result<(), Box<dyn Error>> {
        // remove 'name' from self.environments
        self.environments.remove(name);
        Ok(())
    }
}

pub struct Environment {
    dht_hash_characters: Option<u8>,
    hash_bits: u8,
    image_projection: u16,
    status: EnvironmentStatus,
}
