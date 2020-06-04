use rusqlite::{Connection, ToSql};

use crate::album::Extent;

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
"INSERT INTO files (image_id, pixel_coverage, subdataset)
VALUES (?1, ?2, ?3)";

const ID_SELECT_STMT: &str =
"SELECT id from images WHERE geohash = ?1 AND tile = ?2";

const LIST_SELECT_STMT: &str =
"SELECT cloud_coverage, geohash, pixel_coverage,
    platform, source, subdataset, tile, timestamp
FROM images JOIN files ON images.id = files.image_id";

const LIST_ORDER_BY_STMT: &str =
" ORDER BY images.tile, images.geohash, images.timestamp, files.subdataset";

const SEARCH_SELECT_STMT: &str =
"SELECT COUNT(*) as count, SUBSTR(geohash, 0, REPLACE_LENGTH) as geohash_search, platform, LENGTH(geohash) as precision, source FROM images";

const SEARCH_GROUP_BY_STMT: &str =
" GROUP BY geohash_search, platform, precision, source";

pub struct AlbumIndex {
    conn: Mutex<Connection>,
    id: i64,
}

impl AlbumIndex {
    pub fn new() -> AlbumIndex {
        // initialize sqlite connection - TODO error
        let conn = Connection::open_in_memory().unwrap();
        conn.execute(CREATE_FILES_TABLE_STMT, rusqlite::params![]).unwrap();
        conn.execute(CREATE_IMAGES_TABLE_STMT, rusqlite::params![]).unwrap();
        //conn.execute(CREATE_INDEX_STMT, rusqlite::params![]).unwrap();

        AlbumIndex {
            conn: Mutex::new(conn),
            id: 1000,
        }
    }

    pub fn load(&mut self, cloud_coverage: Option<f64>, geohash: &str,
            pixel_coverage: f64, platform: &str, source: &str,
            subdataset: u8, tile: &str, timestamp: i64) 
            -> Result<(), Box<dyn Error>> {
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
                id, pixel_coverage, subdataset
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
