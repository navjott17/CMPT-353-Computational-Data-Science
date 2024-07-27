import sys
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

filename1 = sys.argv[1]
filename2 = sys.argv[2]

data1 = pd.read_csv(filename1, sep=' ', header=None, index_col=1, names=['lang', 'page', 'views_data1', 'bytes'])
data2 = pd.read_csv(filename2, sep=' ', header=None, index_col=1, names=['lang', 'page', 'views_data2', 'bytes'])

sorted_data1 = data1.sort_values(by='views_data1', ascending=False)

plt.figure(figsize=(10, 5))
plt.subplot(1, 2, 1)
plt.plot(sorted_data1['views_data1'].values)
plt.title("Popularity Distribution")
plt.xlabel("Rank")
plt.ylabel("Views")

series1 = pd.Series(data1['views_data1'])
series2 = pd.Series(data2['views_data2'])
df = pd.concat([series1, series2], axis=1)
plt.subplot(1, 2, 2)
plt.plot(df['views_data1'].values, df['views_data2'].values, 'b.')
plt.xscale('log')
plt.yscale('log')
plt.title("Hourly Correlation")
plt.xlabel("Hour 1 views")
plt.ylabel("Hour 2 views")
plt.savefig('wikipedia.png')


