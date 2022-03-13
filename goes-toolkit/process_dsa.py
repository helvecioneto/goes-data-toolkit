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

        array_ = array.copy()

        # Find in array values equal 32767 and replace with NaN
        array_[array_ == 32767] = 0

        # Apply scale factor
        array_ = array_ / 10000

        # Boundbox
        bbox = [-100.2, -55.3, -25.2, 12.3]

        # Crop Boundbox
        cbbox = list(map(float, os.getenv("bbox").split(",")))

        xres = abs(bbox[0]-bbox[2]) / array_.shape[1]
        yres = abs(bbox[1]-bbox[3]) / array_.shape[0]

        # Set geo transform
        geotransform = (bbox[0], xres, 0, bbox[3], 0, -yres)

        driver = gdal.GetDriverByName('netCDF')
        raw = driver.Create(temp_output + '.nc', array_.shape[1], array_.shape[0], 1, gdal.GDT_Float32)
        raw.SetGeoTransform(geotransform)
        raw.GetRasterBand(1).WriteArray(array_)

        # Define the parameters of the output cropped image
        kwargs = {'format': 'netCDF',
                  'outputBounds': (cbbox[0], cbbox[3], cbbox[2], cbbox[1]),
                  'outputType': gdal.GDT_Float32}

        # Write the reprojected file on disk
        gdal.Warp(temp_output+'.temp1.nc', raw, **kwargs)

        # Close file
        file.close()

        # Delete temp file
        os.remove(temp_output)

        # Remove temp_output + '.nc'
        os.remove(temp_output + '.nc')

        # Transform timestamp to julian day
        julian_day = timestamp.timetuple().tm_yday

        # Transform timestamp to hour of day and minute of hour
        hour_of_day = timestamp.strftime('%H')
        minute_of_hour = timestamp.strftime('%M')
        time_of_day = int(hour_of_day + minute_of_hour)

        # Read netCDF using nc
        nc = netCDF4.Dataset(temp_output + '.temp1.nc', 'r+')

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
        os.rename(temp_output + '.temp1.nc', output_dir + output_file)

    except:
        print('Error: ' + timestamp)
        return 1
