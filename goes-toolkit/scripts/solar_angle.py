import pvlib
import pandas as pd

latitude = 0.0
longitude = -74.99995422
time = pd.to_datetime('2014-08-01 12:00:00')
julian_day = time.timetuple().tm_yday

print('latitude: ' + str(latitude))
print('longitude: ' + str(longitude))
print('julian_day: ' + str(julian_day))
print('time: ' + str(time))
print('---------------\n')

# Convert time to pandas.DatetimeIndex)
time = pd.DatetimeIndex([time])

equation_of_time = pvlib.solarposition.equation_of_time_spencer71(julian_day)
print('equation_of_time: ' + str(equation_of_time))

declination = pvlib.solarposition.declination_spencer71(julian_day)
print('declination: ' + str(declination))

solar_position = pvlib.solarposition.hour_angle(time, longitude, equation_of_time)
print('solar_position: ' + str(solar_position))

zenith = pvlib.solarposition.solar_zenith_analytical(latitude, solar_position, declination)
print('zenith: ' + str(zenith))
