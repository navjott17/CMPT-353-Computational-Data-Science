import sys

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

filename1 = sys.argv[1]
filename2 = sys.argv[2]
filename3 = sys.argv[3]

stations = pd.read_json(filename1, lines=True)
city = pd.read_csv(filename2)


def haversine_formula(city_lat, city_lon, stations_lat, stations_lon):
    # referenced from https://www.tomaszezula.com/2014/09/14/haversine-formula-application-in-python/
    # referenced from https://stackoverflow.com/questions/34510749/python-function-to-calculate-distance-using-haversine-formula-in-pandas
    city_lat_radians = np.radians(city_lat)
    city_lon_radians = np.radians(city_lon)
    stations_lat_radians = np.radians(stations_lat)
    stations_lon_radians = np.radians(stations_lon)
    dist_lat = stations_lat_radians - city_lat_radians
    dist_lon = stations_lon_radians - city_lon_radians

    # central angle
    a = np.sin(dist_lat / 2) ** 2 + np.cos(city_lat_radians) * np.cos(stations_lat_radians) * np.sin(dist_lon / 2) ** 2
    # determinative angle
    c = 2 * np.arcsin(np.sqrt(a))
    # radius of earth in km
    r = 6371
    return r * c


def distance(city, stations):
    dist = haversine_formula(city.latitude, city.longitude, stations['latitude'], stations['longitude'])
    return dist


def best_tmax(city, stations):
    dist = distance(city, stations)
    minimum_index = dist.idxmin(axis=1)
    #reference from https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html
    minimum_value = stations.loc[minimum_index].avg_tmax
    return minimum_value


stations['avg_tmax'] = stations['avg_tmax']/10

city = city.dropna(subset=['population', 'area'])
city['area'] = city['area'] / 1000000
city = city.drop(city[city['area'] > 10000].index)
city['density'] = city['population'] / city['area']

city['avg_tmax'] = city.apply(best_tmax, stations=stations, axis=1)

plt.plot(city['avg_tmax'], city['density'], 'b.')
plt.title('Temperature vs Population Density')
plt.xlabel('Avg Max Temperature (\u00b0C)')
plt.ylabel('Population Density (people/km\u00b2)')
plt.savefig(filename3)
