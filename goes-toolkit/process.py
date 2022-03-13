import os
import logging
from multiprocessing import Pool
from check_error import checkerrors
from filter_data import filterdata
from download_files import download_file
from process_noaa import process_noaa

import warnings
warnings.filterwarnings("ignore")


def process_files():
    """Download files from the server"""

    # Call checkerror function
    checkerrors()

    # Call filterdata function
    filtered_df = filterdata().reset_index()

    # Get parameters
    provider = os.getenv('p')
    output = os.getenv('o') + '/' + provider

    # Check if output dir exist
    if not os.path.exists(output):
        os.makedirs(output)

    # Temp dir
    temp_dir = os.getcwd() + '/temp'

    # Check if temp dir exist
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # Check if logs dir exist
    if not os.path.exists(os.getcwd() + '/logs/'):
        os.makedirs(os.getcwd() + '/logs/')

    # loggin file
    logging.basicConfig(filename=os.getcwd() + '/logs/log.txt', filemode="w",
                        format="%(message)s",
                        datefmt="%d-%b-%y %H:%M:%S",
                        level=logging.INFO)

    # Multiple parallel download
    parallel_downloads = 4

    # Make the Pool of workers
    pool = Pool(parallel_downloads)

    # Download
    for idx in range(0, len(filtered_df.index), parallel_downloads):

        # for parallel download
        args = []
        for i in range(parallel_downloads):
            if idx + i < len(filtered_df.index):
                args.append((filtered_df.iloc[idx + i]['url'],
                             filtered_df.iloc[idx + i]['provider'],
                             filtered_df.iloc[idx + i]['timestamp'],
                            output))

        # and return the results
        downloads_ = pool.map(download_file, args)

        # Check if download is successful
        for download in downloads_:
            if download:
                logging.info('Download failed for: ' + str(download))

        # Process downloaded files
        if provider == 'NOAA':
            print('Processing files...')
            procced_files = pool.map(process_noaa, downloads_)

            for proc in procced_files:
                if not proc[0]:
                    logging.info('Processing failed for: ' + str(proc[1]))

        if provider == 'DSA':
            print('Processing files...')
