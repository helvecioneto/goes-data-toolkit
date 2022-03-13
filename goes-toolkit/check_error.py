import os
import pandas as pd


def checkerrors():
    try:
        pd.to_datetime(os.getenv('s'))
    except ValueError as e:
        print('Invalid value for \'-s\' parameter: ', e.args[-1])
        exit(0)

    try:
        pd.to_datetime(os.getenv('e'))
    except ValueError as e:
        print('Invalid value for \'-e\' parameter: ', e.args[-1])
        exit(0)

    sat = os.getenv('sat')
    if sat != 'goes08' and sat != 'goes09' and sat != 'goes10' and sat != 'goes11' and sat != 'goes12' and sat != 'goes13' and sat != 'goes14' and sat != 'goes15' and sat != 'goes16':
        print('Invalid value for \'-s\' parameter: ', sat)
        print('Only accept sat = goes08, goes09, goes10, goes11, goes12, goes13, goes14, goes15, goes16')
        exit(0)

    # Check provider
    provider = os.getenv('p')
    if provider != 'DSA' and provider != 'NOAA':
        print('Invalid value for \'-p\' parameter: ', provider)
        print('Only accept provider = NOAA or DSA')
        exit(0)
