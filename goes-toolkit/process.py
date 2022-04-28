import warnings
import pandas as pd
import os
import logging
import multiprocessing
from check_error import checkerrors
from filter_data import filterdata
from download_files import download_file
from process_aws import process_aws
from pytimeparse.timeparse import timeparse
import sys
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
    parallel_processes = 8
    pool = multiprocessing.Pool(parallel_processes)
    interval = timeparse(os.getenv('i'))
    between_times = list(map(str, os.getenv("bt").split(",")))

    # Main AWS Frame
    main_aws_df = pd.DataFrame()

    # Check if provider is AWS
    if provider == 'AWS':
        print('Mount AWS Files Frame...')

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

        # Check if is AWS
        if provider == 'AWS':
            for flag in flags:
                # Append to main AWS df
                main_aws_df = pd.concat([main_aws_df, flag])
        else:
            # Check if flags are 0
            for flag in flags:
                if flag == 0:
                    logging.info(f'Error downloading file: {filtered_df.iloc[idx + i]["timestamp"]}')
                    continue
                elif flag == 1:
                    logging.info(f'Downloaded file: {filtered_df.iloc[idx + i]["timestamp"]}')
                    continue

    # Check if provider is AWS
    if provider == 'AWS':
        print('\nProcessing AWS Files...')

        # Keep duplicates first in column timestamp
        main_aws_df = main_aws_df.sort_values(by=['timestamp'], ascending=True).drop_duplicates(subset=['timestamp'], keep='first')

        # Add provider column to main_aws_df
        main_aws_df['provider'] = 'AWS'

        # Set s_scan as index
        main_aws_df = main_aws_df.set_index('timestamp')

        # Group
        # Lock by timedelta frequency at interval seconds
        main_aws_df = main_aws_df.groupby(pd.Grouper(freq=str(interval) + 's')).first()

        # Ignore times between midnight and 6am at guid_df index timestamp
        if between_times[0] != 'None':
            between_times = [pd.to_datetime(tm).strftime('%H:%M:%S') for tm in between_times]
            # Lock by between_times at index of guide_df
            main_aws_df = main_aws_df.between_time(between_times[0], between_times[1])

        # Reset index
        main_aws_df = main_aws_df.reset_index()

        # Process files
        for idx in range(0, len(main_aws_df.index), parallel_processes):

            # for parallel download
            args = []
            for i in range(parallel_processes):
                if idx + i < len(main_aws_df.index):
                    args.append((main_aws_df.iloc[idx + i]['url'],
                                 main_aws_df.iloc[idx + i]['provider'],
                                 main_aws_df.iloc[idx + i]['timestamp'],
                                 output))

            # Download files
            aws_download_parallel = pool.map(download_aws, args)

            # Process files
            for aws_download in aws_download_parallel:
                print('Processing file: ' + str(aws_download[2]))
                if aws_download[0] == 0:
                    logging.info(f'Error downloading file: {aws_download[2]}')
                    continue
                else:
                    process_aws(aws_download[0], aws_download[1], aws_download[2])


def download_aws(args):
    """Download files from AWS"""

    url, provider, timestamp, output = args

    # Output temp file
    temp_output = './temp/' + timestamp.strftime('%Y%m%d_%H%M%S.nc')

    # Status
    attempts = 0

    print(f'Downloading {provider} {timestamp}')

    # Try download while file size is not equal to 0
    while attempts <= 5:
        try:
            # Download file
            os.system('wget --quiet --show-progress -cO - ' + url + ' > ' + temp_output)
            attempts = 6
        except:
            attempts += 1
            # Write in logfile
            print('\nError downloading file: ', timestamp, '\nFrom :', url)
            with open('./logs/' + str(timestamp.strftime('%Y%m%d_%H%M%S')) + '.file_not_found.log', 'w') as f:
                f.write(str(sys.exc_info()))

    return temp_output, output, timestamp
