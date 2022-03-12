import requests
from threading import Thread
import pandas as pd
import os


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
            url_list.append(url + year + month + '/' + file_)

    # Create a dataframe
    output_df['year'] = year_list
    output_df['month'] = month_list
    output_df['file'] = file_list
    output_df['url'] = url_list
    output_df['provider'] = 'NOAA'
    output_df['channel'] = None

    # Write the dataframe to a csv file
    output_df.to_csv('files/noaa/noaa_'+year+'_'+month+'.csv', index=False)


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

    # Concat dsa and noaa dataframes
    output_df = pd.concat([dsa_df_goes13, noaa_df])

    # Drop columns year, month and file
    output_df = output_df.drop(['year', 'month', 'file'], axis=1)

    # Set index
    output_df = output_df.set_index('timestamp')

    # Save pickle file as compression
    output_df.to_pickle('../file_guide.pkl', compression='gzip')


if __name__ == '__main__':

    # Mkdir files
    if not os.path.exists('files/dsa/'):
        os.makedirs('files/dsa/')
    if not os.path.exists('files/noaa/'):
        os.makedirs('files/noaa/')

    # print('Getting the list Data in NOAA...')
    # get_noaa_satl_list()

    # print('Getting the list Data in DSA...')
    # get_dsa_goes13_list()

    print('Merging all the csv files...')
    merge_all_files()

    # # Delete directory files
    # os.system('rm -rf files')

    # print('Done!')
