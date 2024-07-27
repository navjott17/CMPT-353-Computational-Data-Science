
import pandas as pd


totals = pd.read_csv('totals.csv').set_index(keys=['name'])
counts = pd.read_csv('counts.csv').set_index(keys=['name'])


sum_totals_y = totals.sum(axis=1)
min_totals = sum_totals_y.idxmin()
print("City with lowest total precipitation:")
print(min_totals)


sum_totals_x = totals.sum(axis=0)
sum_counts_x = counts.sum(axis=0)
avg_precip_month = sum_totals_x/sum_counts_x
print("Average precipitation in each month:")
print(avg_precip_month)


sum_totals_y = totals.sum(axis=1)
sum_counts_y = counts.sum(axis=1)
avg_precip_city = sum_totals_y/sum_counts_y
print("Average precipitation in each city:")
print(avg_precip_city)

