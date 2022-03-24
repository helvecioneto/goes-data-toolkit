import os
import netCDF4
from osgeo import gdal, osr
import sys
import numpy as np


def process_aws(temp_output, output, timestamp, c_scan, s_scan):

    # try:
    # Parameters
    var_name = 'CMI'

    output_dir = output + '/' + var_name + '/' + timestamp.strftime('%Y') + \
        '/' + timestamp.strftime('%m') + '/'

    # Output file
    output_file = timestamp.strftime('%Y%m%d_%H%M.nc')

    # Verify output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Boundbox
    bbox = list(map(float, os.getenv("bbox").split(",")))

    # Using netCDF to open the file
    nc_geo = netCDF4.Dataset(temp_output, 'r')
    geospatial_lat_long = nc_geo['geospatial_lat_lon_extent']

    # Get the lat and long values
    lat_location = geospatial_lat_long.geospatial_lat_center
    lon_location = geospatial_lat_long.geospatial_lon_center

    # Read temp file
    img = gdal.Open(f'NETCDF:'+temp_output + ":" + var_name)

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
    valid_range = metadata.get(var_name + '#valid_range')
    # _FillValue
    _FillValue = metadata.get(var_name + '#_FillValue')

    # Get global attributes
    global_attributes = img.GetMetadata_Dict()
    global_dict = {}
    for k in global_attributes.keys():
        # Check if is a NC_GLOBAL attribute
        if k.startswith('NC_GLOBAL'):
            # Add key and value to dictionary
            global_dict[k] = global_attributes[k]

    #### REPROJECTION ####
    # Read the original file projection and configure the output projection
    source_prj = osr.SpatialReference()
    source_prj.ImportFromProj4(img.GetProjectionRef())
    target_prj = osr.SpatialReference()
    target_prj.ImportFromProj4(os.getenv('proj4'))

    # Define the parameters of the output file
    kwargs = {'format': 'netCDF',
              'srcSRS': source_prj,
              'dstSRS': target_prj,
              'outputBounds': (bbox[0], bbox[3], bbox[2], bbox[1]),
              'outputBoundsSRS': target_prj,
              'xRes': 0.01,
              'yRes': 0.01,
              'resampleAlg': gdal.GRA_NearestNeighbour  # Reamostragem NearestNeighbour
              }

    # Write the reprojected file on disk
    gdal.Warp(temp_output, img, **kwargs)

    # close the original file
    img.FlushCache()

    # Add variables from metadata atributes
    os.system("ncrename -h -O -v Band1," + var_name + " " + temp_output)
    os.system("ncatted -O -a long_name," + var_name + ",o,c,\""+str(long_name)+"\" "+str(temp_output))
    os.system("ncatted -O -a coordinates," + var_name + ",o,c,\""+str(coordinates)+"\" "+str(temp_output))
    os.system("ncatted -O -a units," + var_name + ",o,f,\""+str(units)+"\" "+str(temp_output))
    os.system("ncatted -O -a scale_factor," + var_name + ",o,f,\""+str(scale_factor)+"\" "+str(temp_output))
    os.system("ncatted -O -a add_offset," + var_name + ",o,f,\""+str(add_offset)+"\" "+str(temp_output))
    os.system("ncatted -O -a valid_range," + var_name + ",o,c,\""+str(valid_range)+"\" "+str(temp_output))
    os.system("ncatted -O -a _FillValue," + var_name + ",o,s,\""+str(_FillValue)+"\" "+str(temp_output))

    # Add global attributes
    os.system("ncatted -O -a processed,global,o,c,\" by: Helvecio B. L. Neto (helvecioblneto@gmail.com)\" "+str(temp_output))

    # Add global_dict to attributes
    for k in global_dict.keys():
        os.system("ncatted -O -a " + k[10:] + ",global,o,c,\""+str(global_dict[k])+"\" "+str(temp_output))

    # Add variables
    # Read netCDF using nc
    nc = netCDF4.Dataset(temp_output, 'r+')

    # Add variable satlat
    nc.createDimension('satlat', 1)
    nc.createVariable('satlat', 'f4', ('satlat',))
    nc.variables['satlat'][:] = lat_location
    nc.variables['satlat'].long_name = 'Satellite Latitude'
    nc.variables['satlat'].units = 'degrees_north'

    # Add variable satlon
    nc.createDimension('satlon', 1)
    nc.createVariable('satlon', 'f4', ('satlon',))
    nc.variables['satlon'][:] = lon_location
    nc.variables['satlon'].long_name = 'Satellite Longitude'
    nc.variables['satlon'].units = 'degrees_east'

    # Add variable julian_day
    nc.createDimension('julian_day', 1)
    nc.createVariable('julian_day', 'i2', ('julian_day',))
    nc.variables['julian_day'][:] = int(timestamp.timetuple().tm_yday)
    nc.variables['julian_day'].long_name = 'Julian day'
    nc.variables['julian_day'].units = 'day'
    nc.variables['julian_day'].comment = str(timestamp.timetuple().tm_yday)

    # Transform timestamp to hour of day and minute of hour
    hour_of_day = timestamp.strftime('%H')
    minute_of_hour = timestamp.strftime('%M')
    time_of_day = str(hour_of_day + minute_of_hour)

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
    os.rename(temp_output, output_dir + output_file)

    # except:
    #     # Write file at './logs/+output_file+'
    #     with open('./logs/' + output_file + '.translate_error.log', 'w') as f:
    #         f.write(str(sys.exc_info()))
