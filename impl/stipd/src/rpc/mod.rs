use tonic::{Code, Status};

pub mod album;
pub mod data;
pub mod node;
pub mod task;

use crate::album::{Album, AlbumManager};

use std::sync::{Arc, RwLock};

pub fn assert_album_exists(album_manager: &Arc<RwLock<AlbumManager>>,
        album: &str) -> Result<Arc<RwLock<Album>>, Status> {
    let album_manager = album_manager.read().unwrap();
    match album_manager.get(album) {
        Some(album) => Ok(album.clone()),
        None => return Err(Status::new(Code::InvalidArgument,
            format!("album '{}' does not exist", album))),
    }
}

pub fn assert_album_not_exists(album_manager: &Arc<RwLock<AlbumManager>>,
        album: &str) -> Result<(), Status> {
    let album_manager = album_manager.read().unwrap();
    match album_manager.get(album) {
        Some(_) => return Err(Status::new(Code::AlreadyExists,
            format!("album '{}' does not exist", album))),
        None => Ok(()),
    }
}
