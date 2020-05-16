use gdal::metadata::Metadata;
use gdal::raster::{Dataset, Driver};
use rusqlite::{Connection, ToSql};

use std::error::Error;
use std::ffi::CString;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Mutex;

pub const FILLED_SOURCE: &'static str = "filled";
pub const RAW_SOURCE: &'static str = "raw";
pub const SPLIT_SOURCE: &'static str = "split";

const CREATE_TABLE_STMT: &str =
"CREATE TABLE images (
    band            TEXT NOT NULL,
    cloud_coverage  FLOAT NULL,
    geohash         TEXT NOT NULL,
    path            TEXT NOT NULL,
    pixel_coverage  FLOAT NOT NULL,
    platform        TEXT NOT NULL,
    source          TEXT NOT NULL,
    timestamp       BIGINT NOT NULL
)";

const INSERT_STMT: &str =
"INSERT INTO images (band, cloud_coverage, geohash, 
        path, pixel_coverage, platform, source, timestamp)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)";

const SELECT_STMT: &str =
"SELECT band, cloud_coverage, geohash, path, 
    pixel_coverage, platform, source, timestamp FROM images";

#[derive(Clone, Debug)]
pub struct ImageMetadata {
    pub band: String,
    pub cloud_coverage: Option<f64>,
    pub geohash: String,
    pub path: String,
    pub pixel_coverage: f64,
    pub platform: String,
    pub source: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub struct Extent {
    pub platform: String,
    pub geohash: String,
    pub band: String,
    pub source: String,
    pub precision: u8,
    pub count: i64,
}

pub struct ImageManager {
    conn: Mutex<Connection>,
    directory: PathBuf,
}

impl ImageManager {
    pub fn new(directory: PathBuf) -> ImageManager {
        // initialize sqlite connection - TODO error
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(CREATE_TABLE_STMT, rusqlite::params![]).unwrap();

        ImageManager {
            conn: Mutex::new(conn),
            directory: directory,
        }
    }

    pub fn get_paths(&self) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        let glob_expression = format!("{}/*/*/*/*/*tif",
            self.directory.to_string_lossy());

        // iterate over existing images
        let mut paths = Vec::new();
        for entry in glob::glob(&glob_expression)? {
            paths.push(entry?);
        }

        Ok(paths)
    }

    pub fn load(&mut self, image: ImageMetadata)
            -> Result<(), Box<dyn Error>> {
        let conn = self.conn.lock().unwrap();
        conn.execute(INSERT_STMT, rusqlite::params![
                image.band, image.cloud_coverage, image.geohash,
                image.path, image.pixel_coverage as f64,
                image.platform, image.source, image.timestamp
            ])?;

        Ok(())
    }

    pub fn write(&mut self, platform: &str, geohash: &str, band: &str, 
            source: &str, tile: &str, timestamp: i64,
            pixel_coverage: f64, dataset: &mut Dataset)
            -> Result<(), Box<dyn Error>> {
        // create directory 'self.directory/platform/geohash/band/dataset'
        let mut path = self.directory.clone();
        for filename in vec!(platform, geohash, band, source) {
            path.push(filename);
            if !path.exists() {
                std::fs::create_dir(&path)?;
                let mut permissions =
                    std::fs::metadata(&path)?.permissions();
                permissions.set_mode(0o755);
                std::fs::set_permissions(&path, permissions)?;
            }
        }

        // check if image path exists
        path.push(tile);
        path.set_extension("tif");

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
        dataset_copy.set_metadata_item("BAND",
            &band.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("CLOUD_COVERAGE",
            &format!("{}", std::f32::MAX), "STIP").unwrap();
        dataset_copy.set_metadata_item("GEOHASH",
            &geohash.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("PIXEL_COVERAGE",
            &pixel_coverage.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("PLATFORM",
            &platform.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("SOURCE",
            &source.to_string(), "STIP").unwrap();
        dataset_copy.set_metadata_item("TIMESTAMP",
            &timestamp.to_string(), "STIP").unwrap();

        // load image into internal store
        self.load(
            ImageMetadata {
                band: band.to_string(),
                cloud_coverage: None,
                geohash: geohash.to_string(),
                path: path_str.to_string(),
                pixel_coverage: pixel_coverage,
                platform: platform.to_string(),
                source: source.to_string(),
                timestamp: timestamp,
            })
    }

    pub fn search(&self, band: &Option<String>, end_timestamp: &Option<i64>,
            geohash: &Option<String>, max_cloud_coverage: &Option<f64>,
            min_pixel_coverage: &Option<f64>, platform: &Option<String>,
            recurse: bool, source: &Option<String>,
            start_timestamp: &Option<i64>) -> Vec<ImageMetadata> {
        // lock the sqlite connection
        let conn = self.conn.lock().unwrap();

        // compile the select command and parameters
        let mut stmt_str = SELECT_STMT.to_string();
        let mut params: Vec<&dyn ToSql> = Vec::new();

        // if exists - filter on band
        if let Some(_) = band {
            params.push(&band);
            stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                true => format!("{} {} band=?{}",
                    stmt_str, "WHERE", params.len()),
                false => format!("{} {} band=?{}",
                    stmt_str, "AND", params.len()),
            };
        }

        // if exists - filter on end_timestamp
        if let Some(_) = end_timestamp {
            params.push(&end_timestamp);
            stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                true => format!("{} {} timestamp<=?{}",
                    stmt_str, "WHERE", params.len()),
                false => format!("{} {} timestamp<=?{}",
                    stmt_str, "AND", params.len()),
            };
        }

        // if exists - filter on geohash
        let geohash_glob = format!("{}%",
            geohash.as_ref().unwrap_or(&"".to_string()));
        if let Some(_) = geohash {
            if recurse {
                params.push(&geohash_glob);
                stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                    true => format!("{} {} geohash LIKE ?{}",
                        stmt_str, "WHERE", params.len()),
                    false => format!("{} {} geohash LIKE ?{}",
                        stmt_str, "AND", params.len()),
                };
            } else {
                params.push(&geohash);
                stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                    true => format!("{} {} geohash=?{}",
                        stmt_str, "WHERE", params.len()),
                    false => format!("{} {} geohash=?{}",
                        stmt_str, "AND", params.len()),
                };
            }
        }

        // if exists - filter on max_cloud_coverage
        if let Some(_) = max_cloud_coverage {
            params.push(&max_cloud_coverage);
            stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                true => format!("{} {} cloud_coverage<=?{}",
                    stmt_str, "WHERE", params.len()),
                false => format!("{} {} cloud_coverage<=?{}",
                    stmt_str, "AND", params.len()),
            };
        }

        // if exists - filter on min_pixel_coverage
        if let Some(_) = min_pixel_coverage {
            params.push(&min_pixel_coverage);
            stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                true => format!("{} {} pixel_coverage>=?{}",
                    stmt_str, "WHERE", params.len()),
                false => format!("{} {} pixel_coverage>=?{}",
                    stmt_str, "AND", params.len()),
            };
        }

        // if exists - filter on platform
        if let Some(_) = platform {
            params.push(&platform);
            stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                true => format!("{} {} platform=?{}",
                    stmt_str, "WHERE", params.len()),
                false => format!("{} {} platform=?{}",
                    stmt_str, "AND", params.len()),
            };
        }

        // if exists - filter on source
        if let Some(_) = source {
            params.push(&source);
            stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                true => format!("{} {} source=?{}",
                    stmt_str, "WHERE", params.len()),
                false => format!("{} {} source=?{}",
                    stmt_str, "AND", params.len()),
            };
        }

        // if exists - filter on start_timestamp
        if let Some(_) = start_timestamp {
            params.push(&start_timestamp);
            stmt_str = match stmt_str.len() == SELECT_STMT.len() {
                true => format!("{} {} timestamp>=?{}",
                    stmt_str, "WHERE", params.len()),
                false => format!("{} {} timestamp>=?{}",
                    stmt_str, "AND", params.len()),
            };
        }

        // execute query - TODO error
        let mut stmt = conn.prepare(&stmt_str).expect("prepare select");
        let images_iter = stmt.query_map(&params, |row| {
            Ok(ImageMetadata {
                band: row.get(0)?,
                cloud_coverage: row.get(1)?,
                geohash: row.get(2)?,
                path: row.get(3)?,
                pixel_coverage: row.get(4)?,
                platform: row.get(5)?,
                source: row.get(6)?,
                timestamp: row.get(7)?,
            })
        }).unwrap();

        images_iter.map(|x| x.unwrap()).collect()
    }

    pub fn search_new(&self, band: &Option<String>, end_timestamp: &Option<i64>,
            geohash: &Option<String>, max_cloud_coverage: &Option<f64>,
            min_pixel_coverage: &Option<f64>, platform: &Option<String>,
            recurse: bool, source: &Option<String>,
            start_timestamp: &Option<i64>) -> Vec<Extent> {
        // lock the sqlite connection
        let conn = self.conn.lock().unwrap();

        // compile the select command and parameters
        //let mut stmt_str = SELECT_STMT.to_string();
        let mut stmt_str = "SELECT platform, SUBSTR(geohash, 0, 2) as geohash_search, band, source, LENGTH(geohash) as precision, COUNT(*) as count FROM images GROUP BY platform, geohash_search, band, source, precision".to_string();
        let mut params: Vec<&dyn ToSql> = Vec::new();

        // execute query - TODO error
        let mut stmt = conn.prepare(&stmt_str).expect("prepare select");
        let extent_iter = stmt.query_map(&params, |row| {
            Ok(Extent {
                platform: row.get(0)?,
                geohash: row.get(1)?,
                band: row.get(2)?,
                source: row.get(3)?,
                precision: row.get(4)?,
                count: row.get(5)?,
            })
        }).unwrap();

        extent_iter.map(|x| x.unwrap()).collect()
    }
}

pub fn to_image_metadata(path: &mut PathBuf)
        -> Result<ImageMetadata, Box<dyn Error>> {
    let dataset = Dataset::open(&path).unwrap();

    // TODO - error
    let timestamp = dataset.metadata_item("TIMESTAMP","STIP")
        .unwrap().parse::<i64>()?;
    let pixel_coverage = dataset.metadata_item("PIXEL_COVERAGE", "STIP")
        .unwrap().parse::<f64>()?;
    let cloud_coverage_dec = dataset.metadata_item("CLOUD_COVERAGE",
        "STIP").unwrap().parse::<f64>()?;

    let cloud_coverage = if cloud_coverage_dec == std::f64::MAX {
        None
    } else {
        Some(cloud_coverage_dec)
    };

    // parse platform and geohash from path
    let path_str = path.to_string_lossy().to_string();
    let _ = path.pop();
    let source = path.file_name()
        .ok_or("source not found in path")?
        .to_string_lossy().to_string();
    let _ = path.pop();
    let band = path.file_name()
        .ok_or("band not found in path")?
        .to_string_lossy().to_string();
    let _ = path.pop();
    let geohash = path.file_name()
        .ok_or("geohash not found in path")?
        .to_string_lossy().to_string();
    let _ = path.pop();
    let platform = path.file_name()
        .ok_or("platform not found in path")?
        .to_string_lossy().to_string();

    // return ImageMetadata
    Ok(ImageMetadata {
        band: band,
        cloud_coverage: cloud_coverage,
        geohash: geohash,
        path: path_str,
        pixel_coverage: pixel_coverage,
        platform: platform,
        source: source,
        timestamp: timestamp,
    })
}
