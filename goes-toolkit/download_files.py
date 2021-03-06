from datetime import datetime
import os
import s3fs
from process_noaa import process_noaa
from process_dsa import process_dsa
from process_dsa16 import process_dsa16

import pathlib
import pandas as pd
import re
from pytimeparse.timeparse import timeparse

import sys


def download_file(args):

    global provider

    """Download file from the server"""
    url, provider, timestamp, output_path, satelite = args

    if url == None:
        return

    # Check provider
    if provider == 'DSA' and satelite == 'goes16':
        print(f'Downloading {provider} {timestamp}', ' from url: ', url)
        # Output temp file
        temp_output = './temp/' + timestamp.strftime('%Y%m%d_%H%M%S.nc')

        # Status
        attempts = 0

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
                return 0

        # Process files
        process_dsa16(temp_output, output_path, timestamp)

    # Check provider
    if provider == 'DSA' and satelite == 'goes13':
        # print('\n\nDownloading file: ', url)
        print(f'Downloading {provider} {timestamp}', ' from url: ', url)

        # Output temp file
        temp_output = './temp/' + timestamp.strftime('%Y%m%d_%H%M%S.gz')

        # Status
        attempts = 0

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
                return 0

    # Check provider
    if provider == 'NOAA':
        # print('\n\nDownloading file: ', timestamp, '\nFrom :', url)

        # Output temp file
        temp_output = './temp/' + timestamp.strftime('%Y%m%d_%H%M%S.nc')

        # Status
        attempts = 0

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
                return 0

        # Begin processing
        process_noaa(temp_output, output_path, timestamp)

    if provider == 'DSA' and satelite == 'goes13':
        process_dsa(temp_output, output_path, timestamp)

    if provider == 'AWS':

        # Get channel os AWS
        channel = os.getenv('c')
        interval = timeparse(os.getenv('i'))
        between_times = list(map(str, os.getenv("bt").split(",")))

        # Sett aws credentials as annonymous
        aws = s3fs.S3FileSystem(anon=True)

        # Get files
        files = aws.ls(url)

        # Create aws frame_df
        frame_df = pd.DataFrame(columns=['c_scan', 'url'])

        for file in files:
            # Get file name
            elements = pathlib.Path(file)
            # Get first element
            buckt_name = elements.parts[0]

            # Secondary path
            path = '/'.join(elements.parts[1:])

            # Get last element
            file_name = elements.parts[-1]

            # Regex if consolidate scan
            consolidate_scan = pd.to_datetime(re.search(r'_c\d+', file_name).group()[2:], format='%Y%j%H%M%S%f')

            # Regex if start_scan
            start_scan = pd.to_datetime(re.search(r'_s\d+', file_name).group()[2:], format='%Y%j%H%M%S%f')

            # Increment 10 minute in timestamp
            timestamp_ = start_scan + pd.Timedelta(minutes=10)

            # Mount url
            url_file = 'https://'+buckt_name+'.s3.amazonaws.com/'+path

            # File name

            # Append to frame_df
            frame_df = frame_df.append({'timestamp': timestamp_,
                                        's_scan': start_scan,
                                        'c_scan': consolidate_scan,
                                        'url': url_file}, ignore_index=True)

        search_strings = ['M6C', 'M3C']
        search_strings = [s+channel for s in search_strings]
        search_strings = '|'.join(search_strings)

        # Filter channel
        frame_df = frame_df[(frame_df['url'].str.contains(search_strings))]

        # Check if frame_df
        if frame_df.empty:
            # Write in logfile
            print('\File not found to download: ', timestamp, '\nFrom :', url)
            print('Writting in logfile')
            # Write file at './logs/+output_file+'
            with open('./logs/' + str(timestamp.strftime('%Y%m%d_%H%M%S')) + '.file_not_found.log', 'w') as f:
                f.write(str(sys.exc_info()))
            return pd.DataFrame()

        # Set s_scan as index
        frame_df = frame_df.set_index('timestamp')

        # Group
        # Lock by timedelta frequency at interval seconds
        frame_df = frame_df.groupby(pd.Grouper(freq=str(interval) + 's')).first()

        # Ignore times between midnight and 6am at guid_df index timestamp
        if between_times[0] != 'None':
            between_times = [pd.to_datetime(tm).strftime('%H:%M:%S') for tm in between_times]
            # Lock by between_times at index of guide_df
            frame_df = frame_df.between_time(between_times[0], between_times[1])

        # Reset index
        frame_df = frame_df.reset_index()

        return frame_df
