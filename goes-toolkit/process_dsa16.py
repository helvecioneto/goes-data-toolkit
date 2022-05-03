import os
from osgeo import gdal


def process_dsa16(temp_output, output_path, timestamp):

    # Get channel
    channel = os.getenv("c")

    var_name = 'Band1'
    output_dir = output_path + '/ch' + channel + '/' + timestamp.strftime('%Y') + \
        '/' + timestamp.strftime('%m') + '/'

    print(output_dir)
    # Output file
    output_file = timestamp.strftime('%Y%m%d_%H%M.nc')

    # Verify output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Boundbox
    bbox = list(map(float, os.getenv("bbox").split(",")))

    # Read temp file
    img = gdal.Open(f'NETCDF:' + temp_output + ":" + var_name)

    # Define the parameters of the output cropped image
    kwargs = {'format': 'netCDF',
              'outputBounds': (bbox[0], bbox[3], bbox[2], bbox[1]),
              'outputType': gdal.GDT_Int32,
              #   'xRes': 0.01,
              #   'yRes': 0.01,
              #   'resampleAlg': gdal.GRA_NearestNeighbour
              }

    # Write the reprojected file on disk
    gdal.Warp(temp_output, img, **kwargs)

    # close the original file
    img.FlushCache()

    # Add variables from metadata atributes
    # os.system("ncrename -h -O -v Band1,ch" + channel + " " + temp_output)

    # Move file to output directory
    os.rename(temp_output, output_dir + output_file)
