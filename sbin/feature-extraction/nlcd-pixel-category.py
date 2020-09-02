#!/bin/python3
import argparse
import gdal
import multiprocessing
import os
import sys

# import relative 'stippy' python project
script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_dir + '/../../../stippy/')
import stippy

def process(image):
    # open dataset
    gdal_dataset = gdal.Open(image[0])
    array = gdal_dataset.GetRasterBand(1).ReadAsArray()

    # aggregate pixel values
    pixels = {}
    for i in range(0, len(array)):
        for j in range(0, len(array[i])):
            if array[i][j] not in pixels:
                pixels[array[i][j]] = 0

            pixels[array[i][j]] += 1

    # initialize image output
    output = image[1]
    for key, value in pixels.items():
        output += ' ' + str(key) + ':' + str(value)

    print(output)

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description='compute image coverage types')
    parser.add_argument('album', type=str, help='stip album')
    parser.add_argument('-i', '--ip-address', type=str,
        help='stip host ip address', default='127.0.0.1')
    parser.add_argument('-p', '--port', type=int,
        help='stip host rpc port', default='15606')
    parser.add_argument('-t', '--thread-count', type=int,
        help='worker thread count', default='8')

    args = parser.parse_args()

    # compile list of processing images
    host_addr = args.ip_address + ':' + str(args.port)
    images = []
    for (node, image) in stippy.list_node_images(host_addr,
            args.album, platform='NLCD'):
        images.append((image.files[0].path, image.geocode))

    # process images
    with multiprocessing.Pool(args.thread_count) as pool:
        pool.map(process, images)
