
import numpy as np


data = np.load('monthdata.npz')
totals = data['totals']
counts = data['counts']

sum_totals_y = np.sum(totals, axis=1)
low_index = np.argmin(sum_totals_y)
print("Row with lowest total precipitation:")
print(low_index)

sum_totals_x = np.sum(totals, axis=0)
sum_counts_x = np.sum(counts, axis=0)
average_precip_month = sum_totals_x / sum_counts_x
print("Average precipitation in each month:")
print(average_precip_month)

sum_totals_y = np.sum(totals, axis=1)
sum_counts_y = np.sum(counts, axis=1)
average_precip_city = sum_totals_y / sum_counts_y
print("Average precipitation in each city:")
print(average_precip_city)

# totals.shape gives a tuple (9,12) here and we need 9 which is 0th index
n = totals.shape[0]
quarters = np.reshape(totals, (4 * n, 3))
quarters_sum_y = np.sum(quarters, axis=1)
quarters_reshape = np.reshape(quarters_sum_y, (n, 4))
print("Quarterly precipitation totals:")
print(quarters_reshape)

