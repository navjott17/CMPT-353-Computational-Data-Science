import time

import numpy as np
import pandas as pd
from implementations import all_implementations


def random_arr():
    rand_arr = np.random.randint(0, 100000, size=4000)
    return rand_arr


final = []

for i in range(150):
    time_list = []
    random_array = random_arr()
    for sort in all_implementations:
        st = time.time()
        res = sort(random_array)
        en = time.time()
        time_list.append(en - st)
    final.append(time_list)

# reference from https://www.geeksforgeeks.org/different-ways-to-create-pandas-dataframe/
data = pd.DataFrame(final, columns=['qs1', 'qs2', 'qs3', 'qs4', 'qs5', 'merge1', 'partition_sort'])
data.to_csv('data.csv', index=False)
