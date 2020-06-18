use rusqlite::{Connection, ToSql};

use crate::{Extent, Image, StFile};
use crate::album::Album;

use std::error::Error;
use std::sync::Mutex;

const CREATE_FILES_TABLE_STMT: &str =
"CREATE TABLE files (
    image_id        BIGINT NOT NULL,
    pixel_coverage  FLOAT NOT NULL,
    subdataset      TINYINT NOT NULL
)";

const CREATE_IMAGES_TABLE_STMT: &str =
"CREATE TABLE images (
    cloud_coverage  FLOAT NULL,
    geocode         TEXT NOT NULL,
    id              BIGINT PRIMARY KEY,
    platform        TEXT NOT NULL,
    source          TEXT NOT NULL,
    tile            TEXT NOT NULL,
    timestamp       BIGINT NOT NULL
)";

//const CREATE_INDEX_STMT: &str =
//"CREATE INDEX idx_images ON images(platform, pixel_coverage)";

const INSERT_FILES_STMT: &str =
"INSERT INTO files (image_id, pixel_coverage, subdataset)
VALUES (?1, ?2, ?3)";

const INSERT_IMAGES_STMT: &str =
"INSERT INTO images (cloud_coverage, geocode,
    id, platform, source, tile, timestamp)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)";

const ID_SELECT_STMT: &str =
"SELECT id from images WHERE geocode = ?1 AND tile = ?2 AND source = ?3";

const LIST_SELECT_STMT: &str =
"SELECT cloud_coverage, geocode, pixel_coverage,
    platform, source, subdataset, tile, timestamp
FROM images JOIN files ON images.id = files.image_id";

const LIST_ORDER_BY_STMT: &str =
" ORDER BY images.timestamp, images.geocode, images.tile, files.subdataset";

const SEARCH_SELECT_STMT: &str =
"SELECT COUNT(*) as count, SUBSTR(geocode, 0, REPLACE_LENGTH) as geocode_search, platform, LENGTH(geocode) as precision, source
FROM (SELECT DISTINCT geocode, platform, source, tile
    FROM images
    JOIN files ON images.id = files.image_id";

const SEARCH_GROUP_BY_STMT: &str =
" ) GROUP BY geocode_search, platform, precision, source";

pub struct AlbumIndex {
    conn: Mutex<Connection>,
    id: i64,
}

impl AlbumIndex {
    pub fn new() -> Result<AlbumIndex, Box<dyn Error>> {
        // initialize sqlite connection
        let conn = Connection::open_in_memory()?;
        conn.execute(CREATE_FILES_TABLE_STMT, rusqlite::params![])?;
        conn.execute(CREATE_IMAGES_TABLE_STMT, rusqlite::params![])?;
        //conn.execute(CREATE_INDEX_STMT, rusqlite::params![])?;

        Ok(AlbumIndex {
            conn: Mutex::new(conn),
            id: 1000,
        })
    }

    pub fn list(&self, album: &Album, end_timestamp: &Option<i64>,
            geocode: &Option<String>, max_cloud_coverage: &Option<f64>,
            min_pixel_coverage: &Option<f64>, platform: &Option<String>,
            recurse: bool, source: &Option<String>,
            start_timestamp: &Option<i64>)
            -> Result<Vec<(Image, Vec<StFile>)>, Box<dyn Error>> {
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

        let geocode_glob = match geocode {
            Some(geocode) => Some(format!("{}%", geocode)),
            None => None,
        };

        match recurse {
            true => append_stmt_filter("geocode", &geocode_glob,
                &mut stmt_str, "LIKE", &mut params),
            false => append_stmt_filter("geocode", geocode,
                &mut stmt_str, "=", &mut params),
        }

        // append LIST_ORDER_BY_STMT to stmt_str
        stmt_str.push_str(LIST_ORDER_BY_STMT);

        // execute query
        let mut stmt = conn.prepare(&stmt_str)?;
        let images_iter = stmt.query_map(&params, |row| {
            let geocode: String = row.get(1)?;
            let platform: String = row.get(3)?;
            let source: String = row.get(4)?;
            let subdataset: u8 = row.get(5)?;
            let tile: String = row.get(6)?;
 
            // TODO - error
            let path = album.get_image_path(false, &geocode,
                &platform, &source, subdataset, &tile).unwrap();

            Ok(((row.get(0)?, geocode, platform,
                    source, tile, row.get(7)?),
                (path.to_string_lossy().to_string(),
                    row.get(2)?, subdataset)))
        })?;

        // process images
        let mut images: Vec<(Image, Vec<StFile>)> = Vec::new();
        for (image, file) in images_iter.map(|x| x.unwrap()) {
            match images.last_mut() {
                Some((i, f)) => {
                    // if geocode and tile match -> append file to files
                    //   else -> add new image
                    match i.1 == image.1 && i.4 == image.4 {
                        true => f.push(file),
                        false => images.push((image, vec!(file))),
                    }
                },
                None => images.push((image, vec!(file))),
            }
        }

        Ok(images)
    }

    pub fn load(&mut self, cloud_coverage: Option<f64>, geocode: &str,
            pixel_coverage: f64, platform: &str, source: &str,
            subdataset: u8, tile: &str, timestamp: i64) 
            -> Result<(), Box<dyn Error>> {
        // load data into sqlite
        let conn = self.conn.lock().unwrap();

        // check if geocode, tile, source combination already exists
        let mut stmt = conn.prepare(ID_SELECT_STMT)?;
        let ids: Vec<i64> = stmt.query_map(
            rusqlite::params![geocode, tile, source],
            |row| { Ok(row.get(0)?) }
        )?.map(|x| x.unwrap()).collect();

        let id = match ids.len() {
            1 => ids[0],
            _ => {
                conn.execute(INSERT_IMAGES_STMT, rusqlite::params![
                    cloud_coverage, geocode, self.id,
                    platform, source, tile, timestamp
                ])?;

                self.id += 1;
                self.id - 1
            },
        };

        conn.execute(INSERT_FILES_STMT, rusqlite::params![
                id, pixel_coverage, subdataset
            ])?;

        Ok(())
    }

    pub fn search(&self, end_timestamp: &Option<i64>,
            geocode: &Option<String>, max_cloud_coverage: &Option<f64>,
            min_pixel_coverage: &Option<f64>, platform: &Option<String>,
            recurse: bool, source: &Option<String>,
            start_timestamp: &Option<i64>)
            -> Result<Vec<Extent>, Box<dyn Error>> {
        // lock the sqlite connection
        let conn = self.conn.lock().unwrap();
 
        // initialize the SELECT command and parameters
        let replace_length = match geocode {
            Some(geocode) => format!("{}", geocode.len() + 2),
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

        let geocode_glob = match geocode {
            Some(geocode) => Some(format!("{}%", geocode)),
            None => None,
        };

        match recurse {
            true => append_stmt_filter("geocode", &geocode_glob,
                &mut stmt_str, "LIKE", &mut params),
            false => append_stmt_filter("geocode", geocode,
                &mut stmt_str, "=", &mut params),
        }

        // append SEARCH_GROUP_BY_STMT to stmt_str
        stmt_str.push_str(SEARCH_GROUP_BY_STMT);

        // execute query
        let mut stmt = conn.prepare(&stmt_str)?;
        let extent_iter = stmt.query_map(&params, |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, 
                row.get(3)?, row.get(4)?))
        })?;

        let extents: Vec<Extent> =
            extent_iter.map(|x| x.unwrap()).collect();

        Ok(extents)
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
