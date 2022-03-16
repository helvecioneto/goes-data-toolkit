import pandas as pd
import os
import pandas as pd
from pytimeparse.timeparse import timeparse
import __init__
from datetime import datetime


def filterdata():

    # Read guide data
    guide_df = pd.read_pickle(__init__.__program_name__+'/file_guide.pkl', compression='gzip')

    # Set parameters
    start_date = pd.to_datetime(os.getenv('s')).to_pydatetime()
    end_date = pd.to_datetime(os.getenv('e')).to_pydatetime()
    sat = os.getenv('sat')
    provider = os.getenv('p')
    interval = timeparse(os.getenv('i'))
    between_times = list(map(str, os.getenv("bt").split(",")))
    channel = os.getenv('c')
    product = os.getenv('prod')

    # lock parameters in guid_df
    guide_df = guide_df[(guide_df['sat'] == sat) & (guide_df['provider'] == provider)]

    # Check if provider is DSA to get channel from list
    if provider == 'DSA' and channel != 'None':
        guide_df = guide_df[(guide_df['channel'] == int(channel))]

    # Check if provider is AWS to get channel from list
    if provider == 'AWS' and product != 'None':
        # Find in url column values that contain product
        guide_df = guide_df[(guide_df['url'].str.contains(product))]

    # Lock by start_date and end_date
    try:
        guide_df = guide_df[(guide_df.index >= start_date) & (guide_df.index <= end_date)].sort_index()
    except:
        # print(guide_df)
        guide_df = guide_df.loc[guide_df.index >= os.getenv('s')].sort_index()
        guide_df = guide_df.loc[guide_df.index <= os.getenv('e')].sort_index()

    # Check if guid_df is empty
    if guide_df.empty:
        print('No data found for this provider, satellite, channel, start_date and end_date')
        print(sat, provider, channel, start_date, end_date)
        exit(0)

    # Transform data_frame.index to datetime
    guide_df.index = pd.to_datetime(guide_df.index)

    # Lock by timedelta frequency at interval seconds
    guide_df = guide_df.groupby(pd.Grouper(freq=str(interval) + 's')).first()

    # Ignore times between midnight and 6am at guid_df index timestamp
    if between_times != ['None']:
        between_times = [pd.to_datetime(tm).strftime('%H:%M:%S') for tm in between_times]

        # Lock by between_times at index of guide_df
        guide_df = guide_df.between_time(between_times[0], between_times[1])

    return guide_df
