import os
import logging
import multiprocessing
from check_error import checkerrors
from filter_data import filterdata
from download_files import download_file

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

    # Create a pool of processes
    parallel_processes = multiprocessing.cpu_count() - 1
    parallel_processes = 6
    pool = multiprocessing.Pool(parallel_processes)

    # Process files
    for idx in range(0, len(filtered_df.index), parallel_processes):

        # for parallel download
        args = []
        for i in range(parallel_processes):
            if idx + i < len(filtered_df.index):
                args.append((filtered_df.iloc[idx + i]['url'],
                             filtered_df.iloc[idx + i]['provider'],
                             filtered_df.iloc[idx + i]['timestamp'],
                            output))

        # Download files
        flags = pool.map(download_file, args)

        # Check if flags are 0
        for flag in flags:
            if flag == 0:
                logging.info(f'Error downloading file: {filtered_df.iloc[idx + i]["timestamp"]}')
            elif flag == 1:
                logging.info(f'Downloaded file: {filtered_df.iloc[idx + i]["timestamp"]}')
