import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd

sorting_data = pd.read_csv('data.csv')

#ANOVA testing means of multiple samples
anova = stats.f_oneway(sorting_data['qs1'], sorting_data['qs2'], sorting_data['qs3'], sorting_data['qs4'],
                       sorting_data['qs5'], sorting_data['merge1'], sorting_data['partition_sort'])
anova_p = anova.pvalue
print(anova_p)

#post hoc analysis
data = pd.DataFrame({'qs1': sorting_data['qs1'], 'qs2': sorting_data['qs2'], 'qs3': sorting_data['qs3'],
                     'qs4': sorting_data['qs4'], 'qs5': sorting_data['qs5'], 'merge1': sorting_data['merge1'],
                     'partition_sort': sorting_data['partition_sort']})
data_melt = pd.melt(data)
post_hoc = pairwise_tukeyhsd(data_melt['value'], data_melt['variable'], alpha=0.05)
print(post_hoc)

# plot = post_hoc.plot_simultaneous()
# plot.savefig('plt.png')
print('Ranking of sorting implementations by speed: partition_sort > qs1 > qs4 > qs5 > qs2 > qs3 > merge1')



