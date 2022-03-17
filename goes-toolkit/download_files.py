import os
import s3fs
from process_noaa import process_noaa
from process_dsa import process_dsa
import pathlib
import pandas as pd
import re
from pytimeparse.timeparse import timeparse


def download_file(args):
    """Download file from the server"""
    url, provider, timestamp, output_path = args

    # Print Download
    print(f'Downloading {provider} {timestamp}')

    if url == None:
        return

    # Check provider
    if provider == 'DSA':
        # print('\n\nDownloading file: ', url)

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

    if provider == 'DSA':
        process_dsa(temp_output, output_path, timestamp)

    if provider == 'AWS':
        print('\n\nDownloading file: ', timestamp, '\nFrom :', url)

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
            start_scan = pd.to_datetime(re.search(r'_s\d+', file_name).group()[2:], format='%Y%j%H%M%S%f').round('min') + pd.Timedelta(minutes=1)

            # Increment 1 minute in timestamp
            timestamp_ = consolidate_scan.round('min')

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

        # Set s_scan as index
        frame_df = frame_df.set_index('s_scan')

        # Ignore times between midnight and 6am at guid_df index timestamp
        if between_times[0] != 'None':
            between_times = [pd.to_datetime(tm).strftime('%H:%M:%S') for tm in between_times]
            # Lock by between_times at index of guide_df
            frame_df = frame_df.between_time(between_times[0], between_times[1])

        # Group
        # Lock by timedelta frequency at interval seconds
        frame_df = frame_df.groupby(pd.Grouper(freq=str(interval) + 's')).first()

        print(frame_df)
