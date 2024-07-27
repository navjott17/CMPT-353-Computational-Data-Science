import difflib
import sys

import numpy as np
import pandas as pd
from pandas import Series

filename1 = sys.argv[1]
filename2 = sys.argv[2]
filename3 = sys.argv[3]

#reference from https://stackoverflow.com/questions/41509435/how-to-read-txt-in-pandas
data1_list = pd.read_csv(filename1, sep=r'\n', engine='python', names=['title'])
data2_ratings = pd.read_csv(filename2)

length = len(data1_list['title'])


def movies_search(movies):
    possibilities = difflib.get_close_matches(movies, data1_list['title'], n=length, cutoff=0.6)
    if len(possibilities) == 0:
        return None
    elif len(possibilities) >= 1:
        return str(possibilities[0])


data2_ratings['best_match'] = data2_ratings['title'].apply(movies_search)
data2_ratings = data2_ratings.dropna(subset=['best_match'])
df = data2_ratings.groupby(['best_match'])
average_ratings = df['rating'].agg(np.mean)
data1_list = data1_list.set_index('title')
data1_list['rating'] = average_ratings.round(2)
data1_list = data1_list.sort_values('title')
#referenced from https://datatofish.com/dropna/#:~:text=%20How%20to%20Drop%20Rows%20with%20NaN%20Values,Step%203%20%28Optional%29%3A%20Reset%20the%20Index%20More%20
data1_list = data1_list.dropna()
#reference from https://www.dataindependent.com/pandas/pandas-to-csv/
data1_list.to_csv(filename3)









