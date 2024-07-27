import math
import sys

from xml.dom.minidom import parse

import numpy as np
import pandas as pd
from pykalman import KalmanFilter


def get_data(filename):
    data = parse(filename)
    data_elements = data.getElementsByTagName('trkpt')
    lat_elements = []
    lon_elements = []
    for i in data_elements:
        latitude = float(i.attributes['lat'].value)
        lat_elements.append(latitude)
        longitude = float(i.attributes['lon'].value)
        lon_elements.append(longitude)

    data_frame = pd.DataFrame(columns=['lat', 'lon'])
    data_frame.lat = lat_elements
    data_frame.lon = lon_elements
    return data_frame


def haversine_formula(lat, lon, adjacent_lat, adjacent_lon):
    # referenced from https://www.tomaszezula.com/2014/09/14/haversine-formula-application-in-python/
    # referenced from https://stackoverflow.com/questions/34510749/python-function-to-calculate-distance-using-haversine-formula-in-pandas
    lat_radians = math.radians(float(lat))
    lon_radians = math.radians(float(lon))
    adj_lat_radians = math.radians(float(adjacent_lat))
    adj_lon_radians = math.radians(float(adjacent_lon))
    dist_lat = adj_lat_radians - lat_radians
    dist_lon = adj_lon_radians - lon_radians

    # central angle
    a = math.sin(dist_lat / 2) ** 2 + math.cos(lat_radians) * math.cos(adj_lat_radians) * math.sin(dist_lon / 2) ** 2
    # determinative angle
    c = 2 * math.asin(math.sqrt(a))
    # radius of earth in km
    r = 6371
    return r * c * 1000


def distance(points):
    #referenced from https://stackoverflow.com/questions/34510749/python-function-to-calculate-distance-using-haversine-formula-in-pandas
    points['adjacent_lat'] = points['lat'].shift(-1)
    points['adjacent_lon'] = points['lon'].shift(-1)
    points['dist'] = points.apply(lambda row: haversine_formula(row['lat'], row['lon'], row['adjacent_lat'],
                                                                row['adjacent_lon']), axis=1)
    total_dist = points['dist'].sum(axis=0)
    return round(total_dist, 2)


def smooth(points):
    kalman_data = points[['lat', 'lon']]*10**5

    initial_state = kalman_data.iloc[0]
    observation_covariance = np.diag([17, 17]) ** 2
    transition_covariance = np.diag([10, 10]) ** 2
    transition = [[1, 0], [0, 1]]

    kf = KalmanFilter(initial_state_mean=initial_state, initial_state_covariance=observation_covariance,
                      observation_covariance=observation_covariance, transition_covariance=transition_covariance,
                      transition_matrices=transition)
    kalman_smoothed, state_cov = kf.smooth(kalman_data)
    kalman_smoothed = kalman_smoothed/10**5
    df = pd.DataFrame(kalman_smoothed, columns=['lat', 'lon'])
    return df


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.8f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.8f' % (pt['lon']))
        trkseg.appendChild(trkpt)

    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)

    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)

    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')


def main():
    points = get_data(sys.argv[1])
    print('Unfiltered distance: %0.2f' % (distance(points)))

    smoothed_points = smooth(points)
    print('Filtered distance: %0.2f' % (distance(smoothed_points)))
    output_gpx(smoothed_points, 'out.gpx')


if __name__ == '__main__':
    main()
