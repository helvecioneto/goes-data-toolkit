 
import glob
import pathlib
import pandas as pd
import numpy as np
import netCDF4 as nc
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

# List all files in the current directory as extensions is .nc
files = glob.glob('../../output/**/*.nc', recursive=True)
# Apply the pathlib.Path class to the list of files
files = [pathlib.Path(f) for f in files]

# Create a dataframe with the file_path and convert file.name to timestamp
df = pd.DataFrame(files, columns=['file_path'])
df['timestamp'] = df['file_path'].apply(lambda x: datetime.strptime(x.name[:-3], '%Y%m%d_%H%M'))
# Sort the dataframe by timestamp
df = df.sort_values(by='timestamp')

# Calculate
for i,row in df.iterrows():
    try:
        print('Running for ',row['file_path'])
        nc_file = nc.Dataset(row['file_path'])
        # Calculated std
        std = nc_file.variables['CMI'][:].std()
        # Calculated mean
        mean = nc_file.variables['CMI'][:].mean()
        # Calculated max
        max = nc_file.variables['CMI'][:].max()
        # Calculated min
        min = nc_file.variables['CMI'][:].min()
        # Get shape of the array
        shape = nc_file.variables['CMI'][:].shape
         # Count number of nan
        nan = nc_file.variables['CMI'][:].filled()
        # Counter number of NaN values into nan matrix
        nan_count = (nan == np.nan).sum()
        # Counter of Fill Values
        fill_count = (nan == -1).sum()
        # Counter of Fill Values
        none_count = (nan < -1).sum()
        # Check matrix shape
        shape = nan.shape

        # Add to i
        df.loc[i, 'std'] = std
        df.loc[i, 'mean'] = mean
        df.loc[i, 'max'] = max
        df.loc[i, 'min'] = min
        df.loc[i, 'nan_count'] = nan_count
        df.loc[i, 'fill_count'] = fill_count
        df.loc[i, 'none_count'] = none_count
        df.loc[i, 'x'] = shape[0]
        df.loc[i, 'y'] = shape[1]
        # Close the file
        nc_file.close()
    except:
        df.loc[i,'error'] = True

# Set timestamp as index
df = df.set_index('timestamp')

df.to_csv('analise.csv')
