use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::metadata::Metadata;
use gdal::raster::{Dataset, Driver};
use rusqlite::{Connection, ToSql};

use std::error::Error;
use std::ffi::CString;
use std::fs::File;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Mutex;

pub const FILLED_SOURCE: &'static str = "filled";
pub const RAW_SOURCE: &'static str = "raw";
pub const SPLIT_SOURCE: &'static str = "split";

const CREATE_FILES_TABLE_STMT: &str =
"CREATE TABLE files (
    image_id        BIGINT NOT NULL,
    path            TEXT NOT NULL,
    pixel_coverage  FLOAT NOT NULL,
    description     TEXT NOT NULL
)";

const CREATE_IMAGES_TABLE_STMT: &str =
"CREATE TABLE images (
    cloud_coverage  FLOAT NULL,
    geohash         TEXT NOT NULL,
    id              BIGINT PRIMARY KEY,
    platform        TEXT NOT NULL,
    source          TEXT NOT NULL,
    tile            TEXT NOT NULL,
    timestamp       BIGINT NOT NULL
)";

//const CREATE_INDEX_STMT: &str =
//"CREATE INDEX idx_images ON images(platform, pixel_coverage)";

const INSERT_IMAGES_STMT: &str =
"INSERT INTO images (cloud_coverage, geohash,
        id, platform, source, tile, timestamp)
    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

const INSERT_FILES_STMT: &str =
"INSERT INTO files (image_id, description, path, pixel_coverage)
    VALUES (?1, ?2, ?3, ?4)";

const ID_SELECT_STMT: &str =
"SELECT id from images WHERE geohash = ?1 AND tile = ?2";

const LIST_SELECT_STMT: &str =
"SELECT cloud_coverage, geohash, path, pixel_coverage,
    platform, source, timestamp FROM images";

const SEARCH_SELECT_STMT: &str =
"SELECT COUNT(*) as count, SUBSTR(geohash, 0, REPLACE_LENGTH) as geohash_search, platform, LENGTH(geohash) as precision, source FROM images";

const SEARCH_GROUP_BY_STMT: &str = "
GROUP BY geohash_search, platform, precision, source";

// count, geohash, platform, precision, source
type Extent = (i64, String, String, u8, String);

// cloud_coverage, geohash, platform, source, tile, timestamp
type Image = (Option<f64>, String, String, String, String, i64);

// path, description
type StFile = (String, String, f64);

#[derive(Clone, Debug)]
pub struct ImageMetadata {
    pub cloud_coverage: Option<f64>,
    pub geohash: String,
    pub files: Vec<FileMetadata>,
    pub pixel_coverage: f64,
    pub platform: String,
    pub source: String,
    pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub struct FileMetadata {
    pub description: String,
    pub path: String,
}

pub struct ImageManager {
    conn: Mutex<Connection>,
    directory: PathBuf,
    id: i64,
}

impl ImageManager {
    pub fn new(directory: PathBuf) -> ImageManager {
        // initialize sqlite connection - TODO error
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(CREATE_FILES_TABLE_STMT, rusqlite::params![]).unwrap();
        conn.execute(CREATE_IMAGES_TABLE_STMT, rusqlite::params![]).unwrap();
        //conn.execute(CREATE_INDEX_STMT, rusqlite::params![]).unwrap();

        ImageManager {
            conn: Mutex::new(conn),
            directory: directory,
            id: 1000,
        }
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
            geohash: &Option<String>, max_cloud_coverage: &Option<f64>,
            min_pixel_coverage: &Option<f64>, platform: &Option<String>,
            recurse: bool, source: &Option<String>,
            start_timestamp: &Option<i64>) -> Vec<ImageMetadata> {
        // lock the sqlite connection
        let conn = self.conn.lock().unwrap();

        // initialize the SELECT command and parameters
        let mut stmt_str = LIST_SELECT_STMT.to_string();
        let mut params: Vec<&dyn ToSql> = Vec::new();

        // append existing filters to stmt_str
        append_stmt_filter("timestamp", end_timestamp,
            &mut stmt_str, "<=", &mut params);
        append_stmt_filter("cloud_coverage", max_cloud_coverage,
            &mut stmt_str, "<=", &mut params);
        append_stmt_filter("pixel_coverage", min_pixel_coverage,
            &mut stmt_str, ">=", &mut params);
        append_stmt_filter("platform", platform,
            &mut stmt_str, "=", &mut params);
        append_stmt_filter("source", source,
            &mut stmt_str, "=", &mut params);
        append_stmt_filter("timestamp", start_timestamp,
            &mut stmt_str, ">=", &mut params);

        let geohash_glob = match geohash {
            Some(geohash) => Some(format!("{}%", geohash)),
            None => None,
        };

        match recurse {
            true => append_stmt_filter("geohash", &geohash_glob,
                &mut stmt_str, "LIKE", &mut params),
            false => append_stmt_filter("geohash", geohash,
                &mut stmt_str, "=", &mut params),
        }

        // TODO - execute query - TODO error
        /*let mut stmt = conn.prepare(&stmt_str).expect("prepare select");
        let images_iter = stmt.query_map(&params, |row| {
            Ok(ImageMetadata {
                cloud_coverage: row.get(0)?,
                geohash: row.get(1)?,
                path: row.get(2)?,
                pixel_coverage: row.get(3)?,
                platform: row.get(4)?,
                source: row.get(5)?,
                timestamp: row.get(6)?,
            })
        }).unwrap();

        images_iter.map(|x| x.unwrap()).collect()*/
        unimplemented!();
    }

    pub fn load(&mut self, cloud_coverage: Option<f64>,
            description: &str, geohash: &str, path: &str,
            pixel_coverage: f64, platform: &str, source: &str,
            tile: &str, timestamp: i64) -> Result<(), Box<dyn Error>> {
        // load data into sqlite
        let conn = self.conn.lock().unwrap();

        // check if tile, geohash combination is already registered
        // execute query - TODO error
        let mut stmt = conn.prepare(ID_SELECT_STMT)
            .expect("prepare id select");
        let ids: Vec<i64> = stmt.query_map(
            rusqlite::params![geohash, tile],
            |row| { Ok(row.get(0)?) }
        ).unwrap().map(|x| x.unwrap()).collect();

        let id = match ids.len() {
            1 => ids[0],
            _ => {
                conn.execute(INSERT_IMAGES_STMT, rusqlite::params![
                    cloud_coverage, geohash, self.id,
                    platform, source, tile, timestamp
                ])?;

                self.id += 1;
                self.id - 1
            },
        };

        conn.execute(INSERT_FILES_STMT, rusqlite::params![
                id, description, path, pixel_coverage
            ])?;

        //println!("LOAD {} {} {} {}", image.4, image.2, image.5, image.6);

        Ok(())
    }

    pub fn search(&self, end_timestamp: &Option<i64>,
            geohash: &Option<String>, max_cloud_coverage: &Option<f64>,
            min_pixel_coverage: &Option<f64>, platform: &Option<String>,
            recurse: bool, source: &Option<String>,
            start_timestamp: &Option<i64>) -> Vec<Extent> {
        // lock the sqlite connection
        let conn = self.conn.lock().unwrap();
 
        // initialize the SELECT command and parameters
        let replace_length = match geohash {
            Some(geohash) => format!("{}", geohash.len() + 2),
            None => "2".to_string(),
        };

        let mut stmt_str = SEARCH_SELECT_STMT
            .replace("REPLACE_LENGTH", &replace_length);
        let mut params: Vec<&dyn ToSql> = Vec::new();

        // append existing filters to stmt_str
        append_stmt_filter("timestamp", end_timestamp,
            &mut stmt_str, "<=", &mut params);
        append_stmt_filter("cloud_coverage", max_cloud_coverage,
            &mut stmt_str, "<=", &mut params);
        append_stmt_filter("pixel_coverage", min_pixel_coverage,
            &mut stmt_str, ">=", &mut params);
        append_stmt_filter("platform", platform,
            &mut stmt_str, "=", &mut params);
        append_stmt_filter("source", source,
            &mut stmt_str, "=", &mut params);
        append_stmt_filter("timestamp", start_timestamp,
            &mut stmt_str, ">=", &mut params);

        let geohash_glob = match geohash {
            Some(geohash) => Some(format!("{}%", geohash)),
            None => None,
        };

        match recurse {
            true => append_stmt_filter("geohash", &geohash_glob,
                &mut stmt_str, "LIKE", &mut params),
            false => append_stmt_filter("geohash", geohash,
                &mut stmt_str, "=", &mut params),
        }

        // append SEARCH_GROUP_BY_STMT to stmt_str
        stmt_str.push_str(SEARCH_GROUP_BY_STMT);

        // execute query - TODO error
        let mut stmt = conn.prepare(&stmt_str).expect("prepare select");
        let extent_iter = stmt.query_map(&params, |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, 
                row.get(3)?, row.get(4)?))
        }).unwrap();

        extent_iter.map(|x| x.unwrap()).collect()
    }

    pub fn write(&mut self, dataset: &mut Dataset, geohash: &str,
            pixel_coverage: f64, platform: &str, source: &str,
            subdataset_number: u8, tile: &str, timestamp: i64)
            -> Result<(), Box<dyn Error>> {
        // create directory 'self.directory/platform/geohash/source'
        let mut path = self.directory.clone();
        for filename in vec!(platform, geohash, source) {
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
        path.push(format!("{}-{}", tile, subdataset_number));
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

        // TODO - load data - TODO - description
        self.load(None, "", geohash, &path.to_string_lossy(),
            pixel_coverage, platform, source, tile, timestamp)?;

        Ok(())
    }
}

fn append_stmt_filter<'a, T: ToSql>(feature: &str, filter: &'a Option<T>,
        stmt: &mut String, op: &str, params: &mut Vec<&'a dyn ToSql>) {
    if let Some(_) = filter {
        params.push(filter);
        let filter_str = match params.len() {
            1 => format!(" WHERE {} {} ?{}", feature, op, params.len()),
            _ => format!(" AND {} {} ?{}", feature, op, params.len()),
        };

        stmt.push_str(&filter_str);
    }
}

pub fn to_image_metadata(path: &mut PathBuf)
        -> Result<(Image, Vec<StFile>), Box<dyn Error>> {
    unimplemented!();
    /*// open input file
    let mut file = File::open(&path)?;

    // read metadata
    let platform = crate::transfer::read_string(&mut file)?;
    let geohash = crate::transfer::read_string(&mut file)?;
    let source = crate::transfer::read_string(&mut file)?;
    let tile = crate::transfer::read_string(&mut file)?;
    let timestamp = file.read_i64::<BigEndian>()?;
    let pixel_coverage = file.read_f64::<BigEndian>()?;

    // read files
    let mut files = Vec::new();
    for _ in 0..file.read_u8()? {
        let path = crate::transfer::read_string(&mut file)?;
        let description = crate::transfer::read_string(&mut file)?;
        files.push((path, description));
    }

    // TODO - read cloud coverage

    Ok(((None, geohash, pixel_coverage, platform,
        source, tile, timestamp), files))*/
}
