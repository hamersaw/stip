use failure::ResultExt;
use gdal::raster::Dataset;
use protobuf::{ImageListRequest, Filter, ImageManagementClient};
use swarm::prelude::Dht;
use tonic::Request;

use crate::{Image, StFile, RAW_SOURCE, SPLIT_SOURCE};
use crate::album::Album;
use crate::task::Task;

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::{Arc, RwLock};

pub struct CoalesceTask {
    album: Arc<RwLock<Album>>,
    dht: Arc<Dht>,
    end_timestamp: Option<i64>,
    geocode: Option<String>,
    max_cloud_coverage: Option<f64>,
    min_pixel_coverage: Option<f64>,
    platform: Option<String>,
    source: Option<String>,
    src_platform: String,
    recurse: bool,
    start_timestamp: Option<i64>,
    window_seconds: i64,
}

impl CoalesceTask {
    pub fn new(album: Arc<RwLock<Album>>, dht: Arc<Dht>,
            end_timestamp: Option<i64>, geocode: Option<String>,
            max_cloud_coverage: Option<f64>,
            min_pixel_coverage: Option<f64>, platform: Option<String>,
            recurse: bool, source: Option<String>, src_platform: String,
            start_timestamp: Option<i64>, window_seconds: i64)
            -> CoalesceTask {
        {
            let album = album.read().unwrap();
            info!("initailizing coalesce task [album={}, end_timestamp={:?}, geocode={:?}, max_cloud_coverage={:?}, min_pixel_coverage={:?}, platform={:?}, recurse={}, source={:?}, src_platform={}, start_timestamp={:?}, window_seconds={}]",
                album.get_id(), end_timestamp, geocode,
                max_cloud_coverage, min_pixel_coverage,
                platform, recurse, source, src_platform,
                start_timestamp, window_seconds);
        }

        CoalesceTask {
            album: album,
            dht: dht,
            end_timestamp: end_timestamp,
            geocode: geocode,
            max_cloud_coverage: max_cloud_coverage,
            min_pixel_coverage: min_pixel_coverage,
            platform: platform,
            recurse: recurse,
            source: source,
            src_platform: src_platform,
            start_timestamp: start_timestamp,
            window_seconds: window_seconds,
        }
    }
}

#[tonic::async_trait]
impl Task<(Image, Vec<StFile>, HashSet<String>)> for CoalesceTask {
    fn process(&self, record: &(Image, Vec<StFile>, HashSet<String>))
            -> Result<(), Box<dyn Error>> {
        let image = &record.0;
        let split_geocodes = &record.2;

        // retrieve album metadata
        let (album_id, dht_key_length, geocode) = {
            let album = self.album.read().unwrap();
            (album.get_id().to_string(), album.get_dht_key_length(),
                album.get_geocode().clone())
        };

        // iterate over split precisions
        let precisions: HashSet<usize> =
            record.2.iter().map(|x| x.len()).collect();

        for precision in precisions {
            for file in record.1.iter() {
                // check if path exists
                let path = {
                    let album = self.album.read().unwrap();
                    album.get_image_path(false, &image.1,
                        &image.2, &image.3, file.2, &image.4)?
                };

                if !path.exists() {
                    return Err(format!("image path '{}' does not exist",
                        path.to_string_lossy()).into());
                }

                // open image
                let dataset = Dataset::open(&path).compat()?;

                // split image with geocode precision
                for dataset_split in st_image::prelude::split(
                        &dataset, geocode, precision)? {
                    // calculate split dataset geocode
                    let (win_min_x, win_max_x, win_min_y, win_max_y) =
                        dataset_split.coordinates();
                    let split_geocode = geocode.get_code(
                        (win_min_x + win_max_x) / 2.0,
                        (win_min_y + win_max_y) / 2.0, precision)?;

                    // skip if geocode is not in split geocodes
                    if !split_geocodes.contains(&split_geocode) {
                        continue; 
                    }

                    // perform dataset split
                    let dataset = dataset_split.dataset()?;

                    // if image has 0.0 coverage -> don't process
                    let pixel_coverage = st_image::coverage(&dataset)?;
                    if pixel_coverage == 0f64 {
                        continue;
                    }

                    // lookup geocode in dht
                    let addr = match crate::task::dht_lookup(
                            &self.dht, dht_key_length, &split_geocode) {
                        Ok(addr) => addr,
                        Err(e) => {
                            warn!("{}", e);
                            continue;
                        },
                    };

                    // send image to new host
                    if let Err(e) = crate::transfer::send_image(&addr,
                            &album_id, &dataset, &split_geocode, file.1,
                            &image.2, SPLIT_SOURCE, file.2, &image.4, image.5) {
                        warn!("failed to write image to node {}: {}", addr, e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn records(&self) -> Result<Vec<(Image, Vec<StFile>, HashSet<String>)>,
            Box<dyn Error>> {
        // retrieve album metadata
        let album_id = {
            let album = self.album.read().unwrap();
            album.get_id().to_string()
        };

        // search for source images using Album
        let src_records: Vec<(Image, Vec<StFile>)> = {
            let album = self.album.read().unwrap();
            album.list(&self.end_timestamp, &self.geocode, &None, &None,
                &Some(self.src_platform.clone()), self.recurse,  
                &Some(RAW_SOURCE.to_string()), &self.start_timestamp)?
        };

        // initialize Filter
        let filter = Filter {
            end_timestamp: self.end_timestamp,
            geocode: self.geocode.clone(),
            max_cloud_coverage: self.max_cloud_coverage,
            min_pixel_coverage: self.min_pixel_coverage,
            platform: self.platform.clone(),
            recurse: self.recurse,
            source: self.source.clone(),
            start_timestamp: self.start_timestamp,
        };

        // initialize ImageListRequest
        let request = ImageListRequest {
            album: album_id,
            filter: filter,
        };

        // iterate over dht nodes
        let mut split_records = HashMap::new();
        for node in self.dht.nodes() {
            // get rpc address
            let addr = format!("http://{}:{}", node.get_ip_address(),
                node.get_metadata("rpc_port").unwrap());

            // open ImageManagementClient
            let mut client = match ImageManagementClient::connect(
                    addr.clone()).await {
                Ok(client) => client,
                Err(e) => return Err(format!(
                    "connection to {} failed: {}", addr, e).into()),
            };

            // send ListImagesRequest
            let mut stream = client.list(Request::new(request.clone()))
                .await?.into_inner();

            // iterate over image stream
            let mut src_index = 0;
            let mut message = stream.message().await?;

            loop {
                // if we have exhausted one list -> break
                if src_index >= src_records.len() || message.is_none() {
                    break;
                }

                // compare the current record pair
                let src_record = &src_records[src_index].0;
                let dst_record = message.as_ref().unwrap();

                if (src_record.5 - dst_record.timestamp).abs() 
                        <= self.window_seconds {
                    if src_record.1 == dst_record.geocode {
                        // geocodes are equal -> increment lowest timestamp
                        if src_record.5 < dst_record.timestamp {
                            src_index += 1;
                        } else {
                            message = stream.message().await?;
                        }
                    } else if dst_record.geocode
                            .starts_with(&src_record.1) {
                        // append pair to split_records
                        let geocodes = split_records.entry(src_index)
                            .or_insert(HashSet::new());
                        geocodes.insert(dst_record.geocode.clone());

                        message = stream.message().await?;
                    } else if src_record.1
                            .starts_with(&dst_record.geocode) {
                        // TODO - merge
                        unimplemented!();
                    } else {
                        // increment lowest geohash
                        if src_record.1 < dst_record.geocode {
                            src_index += 1;
                        } else {
                            message = stream.message().await?;
                        }
                    }
                } else if src_record.5 < dst_record.timestamp {
                    src_index += 1;
                } else {
                    message = stream.message().await?;
                }
            }
        }

        let mut records = Vec::new();
        for (src_index, geocodes) in split_records {
            // TODO - fix clone?
            let record = src_records[src_index].clone();
            records.push((record.0, record.1, geocodes));
        }

        // return list of records
        Ok(records)
    }
}
