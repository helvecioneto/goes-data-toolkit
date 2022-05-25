from osgeo import gdal, ogr,osr
import numpy as np
import gzip
import os


def array2raster(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array):

    cols = array.shape[1]
    rows = array.shape[0]
    originX = rasterOrigin[0]
    originY = rasterOrigin[1]

    driver = gdal.GetDriverByName('netCDF')
    outRaster = driver.Create(newRasterfn, cols, rows, 1, gdal.GDT_Float32)
    outRaster.SetGeoTransform((originX, pixelWidth, 0, originY, 0, pixelHeight))
    outRaster.GetRasterBand(1).WriteArray(array)
    # outRasterSRS = osr.SpatialReference()
    # outRasterSRS.ImportFromEPSG(4326)
    # outRaster.SetProjection(outRasterSRS.ExportToWkt())
    

    # Crop
    crop_bounds = [-40.48,-9.48,-34.6,-5.52]

    # Define the parameters of the output cropped image
    kwargs = {'format': 'netCDF',
              'outputBounds': (crop_bounds[0], crop_bounds[3], crop_bounds[2], crop_bounds[1]),
              'outputType': gdal.GDT_Float32}

    # Write the reprojected file on disk
    gdal.Warp('crop.nc', outRaster, **kwargs)
    # outband.FlushCache()


def main(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array):
    reversed_arr = array[::-1] # reverse array so the tif looks like the array
    array2raster(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,reversed_arr) # convert array to raster


if __name__ == "__main__":
    rasterOrigin = (-100, -56)
    pixelWidth = 0.04
    pixelHeight = 0.04
    newRasterfn = 'test.nc'

    # Read temp file
    file = gzip.open('S10236961_201408011200.gz', mode='rb')
    array = np.frombuffer(file.read(), dtype=np.uint16)
    array = array.reshape((1714, 1870))

    # # Find in array values equal 32767 and replace with NaN
    array = np.where(array == 32767, np.nan, array)

    # # Apply scale factor
    array = array / 1000

    print(np.nanmin(array),np.nanmax(array))

    # array = np.array([[ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    #                   [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    #                   [ 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 1, 1],
    #                   [ 1, 0, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1],
    #                   [ 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 1, 0, 1, 1, 1],
    #                   [ 1, 0, 1, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1],
    #                   [ 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 1],
    #                   [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    #                   [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    #                   [ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]])


    main(newRasterfn,rasterOrigin,pixelWidth,pixelHeight,array)