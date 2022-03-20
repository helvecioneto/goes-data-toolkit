from netCDF4 import Dataset                     # Read / Write NetCDF4 files
import os                                       # Miscellaneous operating system interfaces
from osgeo import osr                           # Python bindings for GDAL
from osgeo import gdal                          # Python bindings for GDAL
import numpy as np                              # Scientific computing with Python
gdal.PushErrorHandler('CPLQuietErrorHandler')   # Ignore GDAL warnings

import matplotlib.pyplot as plt
#-----------------------------------------------------------------------------------------------------------

# Desired extent
extent = [-40.48,-9.48,-34.6,-5.52] # Min lon, Max lon, Min lat, Max lat

#-----------------------------------------------------------------------------------------------------------

file_name = "./testes/originais/sat2132014.1200.nc"

# Var
var = 'imagem_vi'
# Read
img = gdal.Open(f'NETCDF:'+file_name + ":" + var)

# Read the header metadata
metadata = img.GetMetadata()


# # Load the data
array = img.ReadAsArray(0, 0, img.RasterXSize, img.RasterYSize)

# Remove -99 values
array[array == -99] = 0

print(np.nanmax(array),np.nanmin(array),np.nanmean(array))

#-----------------------------------------------------------------------------------------------------------

# Read the original file projection and configure the output projection
source_prj = osr.SpatialReference()
source_prj.ImportFromProj4("+proj=longlat +datum=WGS84 +no_defs")
target_prj = osr.SpatialReference()
target_prj.ImportFromProj4("+proj=longlat +datum=WGS84 +no_defs")

# Reproject the data
GeoT = img.GetGeoTransform()
driver = gdal.GetDriverByName('netCDF')
raw = driver.Create('raw.nc', array.shape[1], array.shape[0], 1, gdal.GDT_Float32)
raw.SetGeoTransform(GeoT)
raw.GetRasterBand(1).WriteArray(array)

# Define the parameters of the output file  
kwargs = {'format': 'netCDF', \
          # 'srcSRS': source_prj, \
          # 'dstSRS': target_prj, \
          'outputBounds': (extent[0], extent[3], extent[2], extent[1]), \
          # 'outputBoundsSRS': target_prj, \
          'outputType': gdal.GDT_Float32,
          # 'srcNodata': -99, \
          # 'dstNodata': -99, \
          # 'xRes': 12000, \
          # 'yRes': 12000, \
          # 'resampleAlg': gdal.GRA_NearestNeighbour
          }

# Write the reprojected file on disk
gdal.Warp(f'recorte_ret.nc', raw, **kwargs)