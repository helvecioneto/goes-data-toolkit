import requests
from threading import Thread
from multiprocessing import Pool
import pandas as pd
import os
import s3fs
import datetime
import time


def get_dsa_goes13_list():
    """
    Get the list of satellites from the NGDC website.
    """

    url = 'http://ftp.cptec.inpe.br/goes/goes13/retangular_4km/'
    response = requests.get(url)

    for channels in response.text.split('\n'):
        if channels.startswith('<tr><td valign="top">') and '/goes/goes13/' not in channels:
            channel = channels.split('"')[7]
            thread = Thread(target=get_channels_dsa, args=((url, channel),))
            thread.start()


def get_channels_dsa(args):
    """
    Get the list of channels in the directory.
    """

    url, channel = args

    response = requests.get(url + channel)
    for years in response.text.split('\n'):
        if years.startswith('<tr><td valign="top">') and '/goes/goes13/' not in years:
            year = years.split('"')[7]
            thread = Thread(target=get_years_dsa, args=((url, channel, year),))
            thread.start()


def get_years_dsa(args):
    url, channel, year = args

    response = requests.get(url + channel + year)
    for months in response.text.split('\n'):
        if months.startswith('<tr><td valign="top">') and '/goes/goes13/' not in months:
            month = months.split('"')[7]
            thread = Thread(target=get_files_dsa, args=((url, channel, year, month),))
            thread.start()


def get_files_dsa(args):

    output_df = pd.DataFrame()

    url_list, channel_list, year_list, month_list, file_list = [], [], [], [], []

    url, channel, year, month = args

    response = requests.get(url + channel + year + month)
    for file in response.text.split('\n'):
        if file.startswith('<tr><td valign="top">') and '/goes/goes13/' not in file:
            file = file.split('"')[7]
            # Append the file to the list
            file_list.append(file)
            # Append the year to the list
            year_list.append(year)
            # Append the month to the list
            month_list.append(month)
            # Append the url to the list
            url_list.append(url + channel + year + month + file)
            # Append channel to the list
            channel_list.append(channel)

    # Create a dataframe
    output_df['year'] = year_list
    output_df['month'] = month_list
    output_df['file'] = file_list
    output_df['url'] = url_list
    output_df['provider'] = 'DSA'
    output_df['channel'] = channel_list

    # Write the dataframe to a csv file
    output_df.to_csv('files/dsa/dsa_'+channel[:-1]+'_'+year[:-1]+'_'+month[:-1]+'.csv', index=False)


def get_noaa_satl_list():
    """
    Get the list of satellites from the NGDC website.
    """

    url = 'https://www.ncei.noaa.gov/data/gridsat-goes/access/goes/'
    response = requests.get(url)

    # Check if the response is OK
    if response.status_code != 200:
        print('Error: Cannot access the www.ncei.noaa.gov.')
        return

    # Add the satellite list to the dictionary
    years_list = []
    for line in response.text.split('\n'):
        if line.startswith('<tr><td><a href="') and '/data/gridsat-goes/access' not in line:
            years_list.append(line.split('"')[1][:-1])

    for year in years_list:
        thread_ = Thread(target=get_noaa_month_dir, args=((url, year),))
        thread_.start()


def get_noaa_month_dir(args):
    """
    Get the list of sub-directories website.
    """
    url, year = args

    response = requests.get(url + year)
    for month in response.text.split('\n'):
        if month.startswith('<tr><td><a href="') and '/data/gridsat-goes/access' not in month:
            month_ = month.split('"')[1][:-1]
            thread = Thread(target=get_noaa_files, args=((url, year, month_),))
            thread.start()


def get_noaa_files(args):
    """
    Get the list of files in the directory.
    """

    output_df = pd.DataFrame()

    url, year, month = args

    url_list, year_list, month_list, file_list = [], [], [], []

    response = requests.get(url + year + '/' + month)
    for file in response.text.split('\n'):
        if file.startswith('<tr><td><a href="') and '/data/gridsat-goes/access' not in file:
            file_ = file.split('"')[1]
            # Append the file to the list
            file_list.append(file_)
            # Append the year to the list
            year_list.append(year)
            # Append the month to the list
            month_list.append(month)
            # Append the url to the list
            url_list.append(url + year + '/' + month + '/' + file_)

    # Create a dataframe
    output_df['year'] = year_list
    output_df['month'] = month_list
    output_df['file'] = file_list
    output_df['url'] = url_list
    output_df['provider'] = 'NOAA'
    output_df['channel'] = None

    # Write the dataframe to a csv file
    output_df.to_csv('files/noaa/noaa_'+year+'_'+month+'.csv', index=False)


def get_aws_goes16():
    print('Getting AWS GOES16 data...')

    # Create a dataframe
    output_df = pd.DataFrame()

    # Seting the bucket
    server = 'noaa-goes16/'
    products = ['ABI-L1b-RadC/', 'ABI-L2-CMIPF/']

    # Range of the years beteen 2017 to current year to string
    years = range(2017, datetime.datetime.now().year)

    # Make a list of julian days
    julian_days = [julian_day for julian_day in range(1, 367)]

    # Range of hours of day
    hours = range(0, 24)

    # list of buckets
    buckets = []
    # list of timestamps
    timestamps = []

    # For each product
    for product in products:
        # For each year
        for year in years:
            # Check if the year is a leap year
            if year % 4 == 0:
                # Append the julian day 366 to julian_days
                julian_days.append(366)
            # For each julian day
            for julian_day in julian_days:
                # For each hour
                for hour in hours:
                    # Create the bucket
                    bucket = server + product + str(year) + '/' + str(julian_day) + '/' + str(hour) + '/'
                    # Strip julian day current datetime check if is a leap year
                    month_day = datetime.datetime.strptime(str(julian_day), '%j').strftime('%m-%d')
                    # Mount timestamp
                    timestamp = str(year) + '-' + month_day + '-' + str(hour) + ':00:00'
                    # Convert timestamp to datetime
                    timestamp = datetime.datetime.strptime(timestamp, '%Y-%m-%d-%H:%M:%S')

                    # Append the bucket to the list
                    buckets.append(bucket)

                    # Append the timestamp to the list
                    timestamps.append(timestamp)

    # Create a dataframe
    output_df['url'] = buckets
    output_df['timestamp'] = timestamps
    output_df['provider'] = 'AWS'
    output_df['channel'] = None
    output_df['file'] = None
    output_df['sat'] = 'goes16'

    # Sort the dataframe by timestamp
    output_df = output_df.sort_values(by='timestamp')

    # Write the dataframe to a csv file
    output_df.to_csv('files/aws/aws_goes16.csv', index=False)


def merge_all_files():
    """
    Merge all the csv files in the directory.
    """
    noaa_df = pd.DataFrame()

    for file in os.listdir('files/noaa/'):
        if file.endswith('.csv'):
            df = pd.read_csv('files/noaa/'+file)
            # Concatenate the dataframe
            noaa_df = pd.concat([noaa_df, df])

    # Processing file guid
    noaa_df['timestamp'] = noaa_df.file.str.extract(r"[.](?P<timestamp>\d+.\d+.\d+.\d+)", expand=False).apply(pd.to_datetime)
    noaa_df['sat'] = noaa_df.file.str.extract(r"[.](?P<sat>\w+)", expand=False)

    dsa_df_goes13 = pd.DataFrame()

    for file in os.listdir('files/dsa/'):
        if file.endswith('.csv'):
            df = pd.read_csv('files/dsa/'+file)
            # Concatenate the dataframe
            dsa_df_goes13 = pd.concat([dsa_df_goes13, df])

    dsa_df_goes13['timestamp'] = dsa_df_goes13.file.str.extract(r"[_](?P<timestamp>\d+.\d+.\d+.\d+)", expand=False).apply(pd.to_datetime)
    dsa_df_goes13['channel'] = dsa_df_goes13.channel.str.extract(r"[ch](?P<timestamp>\d)", expand=False).astype(int)
    dsa_df_goes13['sat'] = 'goes13'

    # AWS S3 DF
    aws_df = pd.DataFrame()

    # Read the csv files
    for file in os.listdir('files/aws/'):
        if file.endswith('.csv'):
            df = pd.read_csv('files/aws/'+file)
            # Concatenate the dataframe
            aws_df = pd.concat([aws_df, df])

    # Concat dsa and noaa dataframes
    output_df = pd.concat([dsa_df_goes13, noaa_df, aws_df])

    # Drop columns year, month and file
    output_df = output_df.drop(['year', 'month', 'file'], axis=1)

    # Set index
    output_df = output_df.set_index('timestamp')

    # Save pickle file as compression
    output_df.to_pickle('../file_guide.pkl', compression='gzip')

    print('File guide created!')
    print(output_df)


if __name__ == '__main__':

    # Mkdir files
    if not os.path.exists('files/dsa/'):
        os.makedirs('files/dsa/')
    if not os.path.exists('files/noaa/'):
        os.makedirs('files/noaa/')
    if not os.path.exists('files/aws/'):
        os.makedirs('files/aws/')

    # print('Getting the list Data in NOAA...')
    # get_noaa_satl_list()
    # print('NOAA list data downloaded!')

    # print('Getting the list Data in DSA...')
    # get_dsa_goes13_list()
    # print('DSA list data downloaded!')

    print('Getting the list Data in AWS...')
    get_aws_goes16()
    print('AWS list data downloaded!')

    print('Merging all the csv files...')
    merge_all_files()

    # # Delete directory files
    # os.system('rm -rf files')

    # print('Done!')
