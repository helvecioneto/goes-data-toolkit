import pandas as pd
import os
import __init__

from filter_data import filterdata


def data_list():
    """
    Get satellite list.
    """
    print('Reading guide data...')
    guide_df = filterdata()

    # Get parameters
    try:
        start_date = pd.to_datetime(os.getenv('s')).strftime('%Y-%m-%d %H:%M:%S')
    except:
        start_date = None
    try:
        end_date = pd.to_datetime(os.getenv('e')).strftime('%Y-%m-%d %H:%M:%S')
    except:
        end_date = None

    # Lock by start_date and end_date
    if start_date or end_date:
        guide_df = guide_df[start_date:end_date].sort_index()

    # Get sat list
    sats = os.getenv('sat')
    provider = os.getenv('p')

    # Check if sat is None
    if sats == 'None':
        sats = guide_df['sat'].unique()
    else:
        sats = [sats]

    # Chck if provider is None
    if provider == 'None':

        provider = guide_df['provider'].unique()
    else:
        provider = [provider]

    # For provider
    for prov in provider:
        print('------------------------------------------------------')
        print('\nProvider: ', prov)
        print('Satellite:\tYears:\t\tMonths:\t\t\t\t\tNumber of files:\n')

        guid_prov = guide_df[guide_df['provider'] == prov]

        for sat in sorted(sats):

            guid_sat = guid_prov[guid_prov['sat'] == sat]

            # Get years
            years = guid_sat[guid_sat['sat'] == sat].index.year.unique().tolist()

            # For year
            for year in sorted(years):

                # Get months
                year_lean_df = guid_sat[guid_sat.index.year == year]

                month = year_lean_df.index.month.unique().tolist()

                # Get number of files
                month_files = year_lean_df.groupby(year_lean_df.index.month).size().tolist()

                if len(month_files) == 1:
                    print(' {}\t\t{}\t\t{}\t\t\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 2:
                    print(' {}\t\t{}\t\t{}\t\t\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 3:
                    print(' {}\t\t{}\t\t{}\t\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 4:
                    print(' {}\t\t{}\t\t{}\t\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 5:
                    print(' {}\t\t{}\t\t{}\t\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 6:
                    print(' {}\t\t{}\t\t{}\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 7:
                    print(' {}\t\t{}\t\t{}\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 8:
                    print(' {}\t\t{}\t\t{}\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 9:
                    print(' {}\t\t{}\t\t{}\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 10:
                    print(' {}\t\t{}\t\t{}\t\t\t{}'.format(sat, year, sorted(month), month_files))
                elif len(month_files) == 11:
                    print(' {}\t\t{}\t{}\t\t{}'.format(sat, year, sorted(month), month_files))
                else:
                    print(' {}\t\t{}\t{}\t\t{}'.format(sat, year, sorted(month), month_files))

            print('------------------------------------------------------')

    print('\nTotal of files in all satelites:', len(guide_df), '\n')
