import gzip
import numpy as np
from osgeo import gdal
import os
import netCDF4


def process_dsa(temp_output, output_path, timestamp):
    """Process DSA files"""

    try:

        #  Parameters
        var_name = 'ch' + os.getenv('c')
        output_dir = output_path + '/' + var_name + '/' + timestamp.strftime('%Y') + \
            '/' + timestamp.strftime('%m') + '/' + timestamp.strftime('%d') + '/'

        # Output file
        output_file = timestamp.strftime('%Y%m%d_%H%M.nc')

        # Verify output directory
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Read temp file
        file = gzip.open(temp_output, mode='rb')
        array = np.frombuffer(file.read(), dtype=np.uint16)
        array = array.reshape((1714, 1870))

        # # Find in array values equal 32767 and replace with NaN
        array = np.where(array == 32767, np.nan, array)

        # # Apply scale factor
        array = array / 1000

        rasterOrigin = (-100, -56)
        pixelWidth = 0.04
        pixelHeight = 0.04

        array2raster(temp_output,rasterOrigin,pixelWidth,pixelHeight,array[::-1]) # convert array to raster

        # Close file
        file.close()

        # Transform timestamp to julian day
        julian_day = timestamp.timetuple().tm_yday

        # Transform timestamp to hour of day and minute of hour
        hour_of_day = timestamp.strftime('%H')
        minute_of_hour = timestamp.strftime('%M')
        time_of_day = int(hour_of_day + minute_of_hour)

        # Read netCDF using nc
        nc = netCDF4.Dataset(temp_output, 'r+')

        # Add variable julian_day
        nc.createDimension('julian_day', 1)
        nc.createVariable('julian_day', 'f4', ('julian_day',))
        nc.variables['julian_day'][:] = julian_day
        nc.variables['julian_day'].long_name = 'Julian day'
        nc.variables['julian_day'].units = 'day'
        nc.variables['julian_day'].comment = str(julian_day)

        # Add variable time_of_day
        nc.createDimension('time_of_day', 1)
        nc.createVariable('time_of_day', 'f4', ('time_of_day',))
        nc.variables['time_of_day'][:] = time_of_day
        nc.variables['time_of_day'].long_name = 'Time of day'
        nc.variables['time_of_day'].units = 'hour and minute'
        nc.variables['time_of_day'].comment = str(time_of_day)

        # Close the file
        nc.close()

        # Move temp_output + '.temp1.nc'
        os.rename(temp_output, output_dir + output_file)

    except:
        print('Error: ' + timestamp)
        return 1


def array2raster(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array):

    cols = array.shape[1]
    rows = array.shape[0]
    originX = rasterOrigin[0]
    originY = rasterOrigin[1]

    driver = gdal.GetDriverByName('netCDF')
    outRaster = driver.Create(newRasterfn + '.raw', cols, rows, 1, gdal.GDT_Float32)
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outRaster.GetRasterBand(1).WriteArray(array)   

    # Crop
    crop_bounds = [-40.48,-9.48,-34.6,-5.52]

    # Define the parameters of the output cropped image
    kwargs = {'format': 'netCDF',
              'outputBounds': (crop_bounds[0], crop_bounds[3], crop_bounds[2], crop_bounds[1]),
              'outputType': gdal.GDT_Float32}

    # Write the reprojected file on disk
    gdal.Warp(newRasterfn, outRaster, **kwargs)

    # Remove raw
    os.remove(newRasterfn + '.raw')