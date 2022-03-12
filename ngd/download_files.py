import os
import pandas as pd
from pytimeparse.timeparse import timeparse

from check_error import check_errors


def download():
    """Download files from the server"""
    print('Downloading files...')

    # Call checkerror function
    check_errors()

    # Read guide data
    guide_df = pd.read_pickle('ngd/file_guide.pkl', compression='gzip')

    # Set parameters
    start_date = pd.to_datetime(os.getenv('s')).strftime('%Y-%m-%d %H:%M:%S')
    end_date = pd.to_datetime(os.getenv('e')).strftime('%Y-%m-%d %H:%M:%S')
    sat = os.getenv('sat')
    provider = os.getenv('p')
    interval = timeparse(os.getenv('i'))
    between_times = list(map(str, os.getenv("bt").split(",")))
    channel = os.getenv('c')

    # lock parameters in guid_df
    guide_df = guide_df[(guide_df['sat'] == sat) & (guide_df['provider'] == provider)]

    # Check if provider is DSA to get channel from list
    if provider == 'DSA':
        guide_df = guide_df[(guide_df['channel'] == int(channel))]

    # Lock by start_date and end_date
    guide_df = guide_df[start_date:end_date].sort_index()

    # Lock by timedelta frequency at interval seconds
    guide_df = guide_df.groupby(pd.Grouper(freq=str(interval) + 's')).first()

    # Ignore times between midnight and 6am at guid_df index timestamp
    if between_times != ['None']:
        between_times = [pd.to_datetime(tm).strftime('%H:%M:%S') for tm in between_times]

        # Lock by between_times at index of guide_df
        guide_df = guide_df.between_time(between_times[0], between_times[1])

    print(guide_df)
