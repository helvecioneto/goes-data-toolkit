import os
import requests


def download_file(args):
    """Download file from the server"""
    url, provider, timestamp, output_path = args

    # Print Download
    print(f'Downloading {provider} {timestamp}')

    # Get file size
    size_of_file = requests.head(url).headers['content-length']

    # Check provider
    if provider == 'DSA':
        # print('\n\nDownloading file: ', url)

        # Output temp file
        temp_output = './temp/' + timestamp.strftime('%Y%m%d_%H%M%S.gz')

        # Status
        attempts = 0

        # Try download while file size is not equal to 0
        while attempts <= 5:
            # Download file
            os.system('wget --quiet --show-progress -cO - ' + url + ' > ' + temp_output)

            # Check if file size is same
            if os.path.getsize(temp_output) != int(size_of_file):
                print('\nFile size is not same. Deleting file: ', temp_output)
                os.remove(temp_output)
                attempts += 1
                failure = True
            else:
                failure = False
                attempts = 6

    # Check provider
    if provider == 'NOAA':
        # print('\n\nDownloading file: ', timestamp, '\nFrom :', url)

        # Output temp file
        temp_output = './temp/' + timestamp.strftime('%Y%m%d_%H%M%S.nc')

        # Status
        attempts = 0

        # Try download while file size is not equal to 0
        while attempts <= 5:
            # Download file
            os.system('wget --quiet --show-progress -cO - ' + url + ' > ' + temp_output)

            # Check if file size is same
            if os.path.getsize(temp_output) != int(size_of_file):
                print('\nFile size is not same. Deleting file: ', temp_output)
                os.remove(temp_output)
                attempts += 1
                failure = True
            else:
                attempts = 6
                failure = False

    return temp_output, failure, timestamp, output_path
