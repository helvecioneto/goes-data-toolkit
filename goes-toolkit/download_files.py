import os
import s3fs
from process_noaa import process_noaa
from process_dsa import process_dsa
import pathlib


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

        # Sett aws credentials as annonymous
        aws = s3fs.S3FileSystem(anon=True)

        files = aws.ls(url)

        for file in files:
            elements = pathlib.Path(file)
            # Get first element
            buckt_name = elements.parts[0]

            # Secondary path
            path = '/'.join(elements.parts[1:])

            # Mount url
            url_file = 'https://'+buckt_name+'.s3.amazonaws.com/'+path

            print(url_file)
