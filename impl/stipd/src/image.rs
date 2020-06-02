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

const CREATE_FILES_TABLE_STMT: &str =
"CREATE TABLE files (
    description     TEXT NOT NULL,
    image_id        BIGINT NOT NULL,
    pixel_coverage  FLOAT NOT NULL,
    subdataset      TINYINT NOT NULL
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
"INSERT INTO files (description, image_id,
    pixel_coverage, subdataset)
VALUES (?1, ?2, ?3, ?4)";

const ID_SELECT_STMT: &str =
"SELECT id from images WHERE geohash = ?1 AND tile = ?2";

const LIST_SELECT_STMT: &str =
"SELECT cloud_coverage, description, geohash, pixel_coverage,
    platform, source, subdataset, tile, timestamp
FROM images JOIN files ON images.id = files.image_id";

const LIST_ORDER_BY_STMT: &str =
" ORDER BY images.tile, images.geohash, files.subdataset";

const SEARCH_SELECT_STMT: &str =
"SELECT COUNT(*) as count, SUBSTR(geohash, 0, REPLACE_LENGTH) as geohash_search, platform, LENGTH(geohash) as precision, source FROM images";

const SEARCH_GROUP_BY_STMT: &str =
" GROUP BY geohash_search, platform, precision, source";

// count, geohash, platform, precision, source
pub type Extent = (i64, String, String, u8, String);

// cloud_coverage, geohash, platform, source, tile, timestamp
pub type Image = (Option<f64>, String, String, String, String, i64);

// description, path, pixel_coverage, subdataset
pub type StFile = (String, String, f64, u8);

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
            start_timestamp: &Option<i64>) -> Vec<(Image, Vec<StFile>)> {
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

        // append LIST_ORDER_BY_STMT to stmt_str
        stmt_str.push_str(LIST_ORDER_BY_STMT);

        // execute query - TODO error
        let mut stmt = conn.prepare(&stmt_str).expect("prepare select");
        let images_iter = stmt.query_map(&params, |row| {
            let geohash: String = row.get(2)?;
            let platform: String = row.get(4)?;
            let source: String = row.get(5)?;
            let subdataset: u8 = row.get(6)?;
            let tile: String = row.get(7)?;
 
            // TODO - error
            let path = self.get_image_path(false, &geohash,
                &platform, &source, subdataset, &tile).unwrap();

            Ok(((row.get(0)?, geohash, platform,
                    source, tile, row.get(8)?),
                (row.get(1)?, path.to_string_lossy().to_string(),
                    row.get(3)?, subdataset)))
        }).unwrap();

        // process images
        let mut images: Vec<(Image, Vec<StFile>)> = Vec::new();
        for (image, mut file) in images_iter.map(|x| x.unwrap()) {
            match images.last_mut() {
                Some((i, f)) => {
                    // if geohash and tile match -> append file to files
                    //   else -> add new image
                    match i.1 == image.1 && i.5 == image.5 {
                        true => f.push(file),
                        false => images.push((image, vec!(file))),
                    }
                },
                None => images.push((image, vec!(file))),
            }
        }

        images
    }

    pub fn load(&mut self, cloud_coverage: Option<f64>,
            description: &str, geohash: &str, pixel_coverage: f64,
            platform: &str, source: &str, subdataset: u8, tile: &str,
            timestamp: i64) -> Result<(), Box<dyn Error>> {
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
                description, id, pixel_coverage, subdataset
            ])?;

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

    pub fn write(&mut self, dataset: &mut Dataset, description: &str,
            geohash: &str, pixel_coverage: f64, platform: &str,
            source: &str, subdataset: u8, tile: &str,
            timestamp: i64) -> Result<(), Box<dyn Error>> {
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
        dataset_copy.set_metadata_item("DESCRIPTION",
            description, "STIP").unwrap();
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

        // load data
        self.load(None, description, geohash, pixel_coverage,
            platform, source, subdataset, tile, timestamp)?;

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
        -> Result<(Image, StFile), Box<dyn Error>> {
    let dataset = Dataset::open(&path).unwrap();

    // TODO - error
    let cloud_coverage =
            match dataset.metadata_item("CLOUD_COVERAGE", "STIP") {
        Some(cloud_coverage) => Some(cloud_coverage.parse::<f64>()?),
        None => None,
    };
    let description = dataset.metadata_item("DESCRIPTION", "STIP").unwrap();
    let geohash = dataset.metadata_item("GEOHASH", "STIP").unwrap();
    let path = path.to_string_lossy().to_string();
    let pixel_coverage = dataset.metadata_item("PIXEL_COVERAGE", "STIP")
        .unwrap().parse::<f64>()?;
    let platform = dataset.metadata_item("PLATFORM", "STIP").unwrap();
    let source = dataset.metadata_item("SOURCE", "STIP").unwrap();
    let subdataset = dataset.metadata_item("SUBDATASET", "STIP")
        .unwrap().parse::<u8>()?;
    let tile = dataset.metadata_item("TILE", "STIP").unwrap();
    let timestamp = dataset.metadata_item("TIMESTAMP", "STIP")
        .unwrap().parse::<i64>()?;

    Ok(((cloud_coverage, geohash, platform, source, tile, timestamp),
        (description, path, pixel_coverage, subdataset)))
}
