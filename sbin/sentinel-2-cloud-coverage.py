#!/bin/python3

import argparse
import gdal
import math
import multiprocessing
import numpy as np
import os
import s2cloudless
import sys

# import realative 'stippy' python project
script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_dir + '/../../stippy/')
import stippy

BANDS = [(2, 1), (0, 1), (0,3), (1, 1),
    (0, 4), (1, 4), (2, 2), (2, 3), (1, 5), (1, 6)]

#def compute_cloud_coverage(directory, platform, geohash, source, tile):
def compute_cloud_coverage(image):
    # compute max width and height
    width = 0
    height = 0
    for (file_index, band_index) in BANDS:
        gdal_dataset = gdal.Open(image.files[file_index].path)

        if gdal_dataset.RasterYSize > height:
            height = gdal_dataset.RasterYSize
        if gdal_dataset.RasterXSize > width:
            width = gdal_dataset.RasterXSize

    #print('image dimension: ' + str(width) + ' x ' + str(height))

    # compile array of image band reflectances
    band_array = [[]]
    for i in range(0, height):
        band_array[0].append([])

        for j in range(0, width):
            band_array[0][i].append([])

    for (file_index, band_index) in BANDS:
        gdal_dataset = gdal.Open(image.files[file_index].path)
        array = gdal_dataset.GetRasterBand(band_index) \
            .ReadAsArray(buf_xsize=width, buf_ysize=height)
        #print('  ' + str(len(array[0])) + ', ' + str(len(array)))

        for i in range(0, height):
            for j in range(0, width):
                band_array[0][i][j].append(array[i][j] / 10000)

    # calculate cloud probability map
    cloud_detector = s2cloudless.S2PixelCloudDetector(all_bands=False)
    cloud_masks = cloud_detector.get_cloud_masks(np.array(band_array))

    # compute ratio of clear and cloud pixels
    cloud_pixels = 0
    clear_pixels = 0
    for i in range(0, height):
        for j in range(0, width):
            if cloud_masks[0][i][j] == 0:
                clear_pixels += 1
            else:
                cloud_pixels += 1

    #print(str(cloud_pixels) + ' ' + str(clear_pixels))
    return cloud_pixels / (cloud_pixels + clear_pixels)

def process(image):
    # validate image
    if len(image.files) != 4:
        print('not all files found')
        return

    cloud_coverage = compute_cloud_coverage(image)
    print(image.geohash + ' ' + str(cloud_coverage))

    # update all existing image bands
    processed = []
    for (file_index, band_index) in BANDS:
        # check if file already processed
        if file_index in processed:
            continue

        # update file
        gdal_dataset = gdal.Open(image.files[file_index].path)
        gdal_dataset.SetMetadataItem("CLOUD_COVERAGE",
            str(cloud_coverage), "STIP")

        processed.append(file_index)

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description='compute cloud coverage')
    parser.add_argument('-i', '--ip-address', type=str,
        help='stip host ip address', default='127.0.0.1')
    parser.add_argument('-p', '--port', type=int,
        help='stip host rpc port', default='15606')
    parser.add_argument('-t', '--thread-count', type=int,
        help='worker thread count', default='4')

    args = parser.parse_args()

    # compile list of processing images
    host_addr = args.ip_address + ':' + str(args.port)
    images = []
    for (node, image) in stippy.list_node_images(
            host_addr, platform='Sentinel-2'):
        images.append(image)

    # process images
    with multiprocessing.Pool(args.thread_count) as pool:
        pool.map(process, images)
