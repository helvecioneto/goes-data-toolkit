import os
import pandas as pd

from check_error import check_errors


def download():
    """Download files from the server"""
    print('Downloading files...')

    # Call checkerror function
    check_errors()

    # Read guide data
    guide_df = pd.read_pickle('ngd/file_guide.pkl', compression='gzip')

    # # Set parameters
    # start_date = pd.to_datetime(os.getenv('start')).strftime('%Y-%m-%d %H:%M:%S')
    # end_date = pd.to_datetime(os.getenv('end')).strftime('%Y-%m-%d %H:%M:%S')
    # sat = os.getenv('sat')
    # interval = os.getenv('interval')

    # # lock parameters in guid_df
    # guide_df = guide_df[(guide_df['sat'] == sat)]
    # guide_df = guide_df[start_date:end_date].sort_index()

    print(guide_df.index)
