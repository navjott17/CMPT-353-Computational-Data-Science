import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.nonparametric.smoothers_lowess import lowess
from pykalman import KalmanFilter

filename1 = sys.argv[1]

cpu_data = pd.read_csv(filename1)

cpu_data.timestamp = pd.to_datetime(cpu_data['timestamp'], format='%Y-%m-%d %H:%M:%S')

loess_smoothed = lowess(cpu_data['temperature'], cpu_data['timestamp'], frac=0.008)
plt.figure(figsize=(12, 4))
plt.plot(cpu_data['timestamp'], cpu_data['temperature'], 'b.', alpha=0.5)
plt.plot(cpu_data['timestamp'], loess_smoothed[:, 1], 'r-')

kalman_data = cpu_data[['temperature', 'cpu_percent', 'sys_load_1', 'fan_rpm']]

initial_state = kalman_data.iloc[0]
observation_covariance = np.diag([2.3, 2.9, 1.2, 29]) ** 2 #fan_rpm is large cz I think fan creates way more noise
transition_covariance = np.diag([0.1, 0.2, 0.1, 0.1]) ** 2
transition = [[0.97, 0.5, 0.2, -0.001], [0.1, 0.4, 2.2, 0], [0, 0, 0.95, 0], [0, 0, 0, 1]]

kf = KalmanFilter(initial_state_mean=initial_state, initial_state_covariance=observation_covariance,
                  observation_covariance=observation_covariance, transition_covariance=transition_covariance,
                  transition_matrices=transition)
kalman_smoothed, state_cov = kf.smooth(kalman_data)
plt.plot(cpu_data['timestamp'], kalman_smoothed[:, 0], 'g-')
plt.xlabel('Timestamp')
plt.ylabel('Temperature')
plt.legend(['Temperature Readings', 'LOESS Smoothing', 'Kalman Smoothing'])
plt.savefig('cpu.svg')


