import os
import sys
import numpy as np
import pandas as pd
import netCDF4
import pvlib
from osgeo import gdal
gdal.PushErrorHandler('CPLQuietErrorHandler')   # Ignore GDAL warnings


def process_noaa(temp_dir, output, timestamp):
    """Process NOAA files"""

    try:
        print('\nProcessing NOAA file: ', timestamp)

        #  Parameters
        var_name = 'ch' + os.getenv('c')
        output_dir = output + '/' + var_name + '/' + timestamp.strftime('%Y') + \
            '/' + timestamp.strftime('%m') + '/'

        # Output file
        output_file = timestamp.strftime('%Y%m%d_%H%M.nc')

        # Verify output directory
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Boundbox
        bbox = list(map(float, os.getenv("bbox").split(",")))

        # Read temp file
        img = gdal.Open(f'NETCDF:'+temp_dir + ":" + var_name)

        # Read the header metadata
        metadata = img.GetMetadata()

        # long_name
        long_name = metadata.get(var_name + '#long_name')
        # coordinates
        coordinates = metadata.get(var_name + '#coordinates')
        # units
        units = metadata.get(var_name + '#units')
        # scale_factor
        scale_factor = metadata.get(var_name + '#scale_factor')
        # add_offset
        add_offset = metadata.get(var_name + '#add_offset')
        # valid_range
        valid_range = metadata.get(var_name + '#valid_range').split(',')
        # Remove the '[' and ']' from the valid_range
        valid_range = [int(valid_range[0][1:]), int(valid_range[1][:-1])]
        # actual_range
        actual_range = metadata.get(var_name + '#actual_range').split(',')
        # Remove the '[' and ']' from the actual_range
        actual_range = [int(actual_range[0][1:]), int(actual_range[1][:-1])]
        # missing_value
        missing_value = metadata.get(var_name + '#missing_value')
        # _FillValue
        _FillValue = metadata.get(var_name + '#_FillValue')
        # coverage_content_type
        coverage_content_type = metadata.get(var_name + '#coverage_content_type')
        # comment
        comment = metadata.get(var_name + '#comment')

        # Get global attributes
        global_attributes = img.GetMetadata_Dict()
        global_dict = {}
        for k in global_attributes.keys():
            # Check if is a NC_GLOBAL attribute
            if k.startswith('NC_GLOBAL'):
                # Add key and value to dictionary
                global_dict[k] = global_attributes[k]

        # Read satlat file
        satlat = gdal.Open(f'NETCDF:'+temp_dir + ":satlat")
        satlat_value = satlat.ReadAsArray()[0][0]
        satlat_meta = satlat.GetMetadata()
        satlat_long_name = satlat_meta.get('satlat#long_name')
        satlat_units = satlat_meta.get('satlat#units')

        # read satlon file
        satlon = gdal.Open(f'NETCDF:'+temp_dir + ":satlon")
        satlon_value = satlon.ReadAsArray()[0][0]
        satlon_meta = satlon.GetMetadata()
        satlon_long_name = satlon_meta.get('satlon#long_name')
        satlon_units = satlon_meta.get('satlon#units')

        # Transform timestamp to julian day
        julian_day = timestamp.timetuple().tm_yday

        # Transform timestamp to hour of day and minute of hour
        hour_of_day = timestamp.strftime('%H')
        minute_of_hour = timestamp.strftime('%M')
        time_of_day = int(hour_of_day + minute_of_hour)

        ####################### PROCESSING ##############################

        # Load the data
        # ds = img.ReadAsArray(0, 0, img.RasterXSize, img.RasterYSize)

        # Filter values undef
        # ds[ds == undef] = 0

        # Apply the scale, offset
        # ds = ds * scale * 1000

        # Get zenith angle
        #zenith = get_zenith(satlat_value, satlon_value, timestamp, julian_day)

        # Apply zenith angle to get the irradiance
        #ds = ds / np.cos(zenith)

        # Convert to int
        # ds = ds.astype(int)

        # # Apply scale, offset, and undef
        # GeoT = img.GetGeoTransform()
        # driver = gdal.GetDriverByName('netCDF')
        # raw = driver.Create(temp_dir, ds.shape[1], ds.shape[0], 1, gdal.GDT_)
        # raw.SetGeoTransform(GeoT)
        # raw.GetRasterBand(1).WriteArray(ds)
        # # Close the file
        # raw.FlushCache()

        # Define the parameters of the output cropped image
        kwargs = {'format': 'netCDF',
                  'outputBounds': (bbox[0], bbox[3], bbox[2], bbox[1]),
                  }

        # Write the reprojected file on disk
        gdal.Warp(temp_dir, img, **kwargs)

        # Add variables from metadata atributes
        os.system("ncrename -h -O -v Band1," + var_name + " " + temp_dir)
        os.system("ncatted -O -a long_name," + var_name + ",o,c,\""+str(long_name)+"\" "+str(temp_dir))
        os.system("ncatted -O -a coordinates," + var_name + ",o,c,\""+str(coordinates)+"\" "+str(temp_dir))
        os.system("ncatted -O -a units," + var_name + ",o,c,\""+str(units)+"\" "+str(temp_dir))
        os.system("ncatted -O -a scale_factor," + var_name + ",o,f,\""+str(scale_factor)+"\" "+str(temp_dir))
        os.system("ncatted -O -a add_offset," + var_name + ",o,f,\""+str(add_offset)+"\" "+str(temp_dir))
        os.system("ncatted -O -a valid_range,"+var_name+",o,s,\""+str(valid_range[0])+","+str(valid_range[1]) + "\" " + str(temp_dir))
        os.system("ncatted -O -a actual_range,"+var_name+",o,f,\""+str(actual_range[0])+","+str(actual_range[1]) + "\" " + str(temp_dir))
        os.system("ncatted -O -a missing_value," + var_name + ",o,s,\""+str(missing_value)+"\" "+str(temp_dir))
        os.system("ncatted -O -a _FillValue," + var_name + ",o,s,\""+str(_FillValue)+"\" "+str(temp_dir))
        os.system("ncatted -O -a coverage_content_type," + var_name + ",o,c,\""+str(coverage_content_type)+"\" "+str(temp_dir))
        os.system("ncatted -O -a comment," + var_name + ",o,c,\""+str(comment)+"\" "+str(temp_dir))

        # Add global attributes
        os.system("ncatted -O -a processed,global,o,c,\" by: Helvecio B. L. Neto (helvecioblneto@gmail.com)\" "+str(temp_dir))

        # Add global_dict to attributes
        for k in global_dict.keys():
            os.system("ncatted -O -a " + k[10:] + ",global,o,c,\""+str(global_dict[k])+"\" "+str(temp_dir))

        # Read netCDF using nc
        nc = netCDF4.Dataset(temp_dir, 'r+')

        # Add variable satlat
        nc.createDimension('satlat', 1)
        nc.createVariable('satlat', 'f4', ('satlat',))
        nc.variables['satlat'][:] = satlat_value
        nc.variables['satlat'].long_name = satlat_long_name
        nc.variables['satlat'].units = satlat_units

        # Add variable satlon
        nc.createDimension('satlon', 1)
        nc.createVariable('satlon', 'f4', ('satlon',))
        nc.variables['satlon'][:] = satlon_value
        nc.variables['satlon'].long_name = satlon_long_name
        nc.variables['satlon'].units = satlon_units

        # Add variable julian_day
        nc.createDimension('julian_day', 1)
        nc.createVariable('julian_day', 'i2', ('julian_day',))
        nc.variables['julian_day'][:] = int(julian_day)
        nc.variables['julian_day'].long_name = 'Julian day'
        nc.variables['julian_day'].units = 'day'
        nc.variables['julian_day'].comment = str(julian_day)

        # Convert time_of_day to string char
        time_of_day_char = netCDF4.stringtochar(np.array([str(time_of_day)], 'S4'))

        # Add variable time_of_day
        nc.createDimension('time_of_day', 4)
        nc.createVariable('time_of_day', 'S1', ('time_of_day',))
        nc.variables['time_of_day'][:] = time_of_day_char
        nc.variables['time_of_day'].long_name = 'Time of day'
        nc.variables['time_of_day'].units = 'hour and minute'
        nc.variables['time_of_day'].comment = str(time_of_day)

        # Close the file
        nc.close()

        # Move file to output directory
        os.rename(temp_dir, output_dir + output_file)

    except:
        # Write file at './logs/+output_file+'
        with open('./logs/' + output_file + '.translate_error.log', 'w') as f:
            f.write(str(sys.exc_info()))


def get_zenith(latitude, longitude, time, julian_day):
    """
    Get the zenith angle of the satellite
    """

    # Convert time to pandas.DatetimeIndex)
    time = pd.DatetimeIndex([time])
    # Equation of time from Duffie & Beckman and attributed to Spencer (1971) and Iqbal (1983).
    equation_of_time = pvlib.solarposition.equation_of_time_spencer71(julian_day)
    # Solar declination from Duffie & Beckman and attributed to Spencer (1971) and Iqbal (1983).
    declination = pvlib.solarposition.declination_spencer71(julian_day)
    # Hour angle in local solar time. Zero at local solar noon.
    solar_position = pvlib.solarposition.hour_angle(time, longitude, equation_of_time)
    # Analytical expression of solar zenith angle based on spherical trigonometry.
    zenith = pvlib.solarposition.solar_zenith_analytical(latitude, solar_position, declination)

    return zenith[0]
