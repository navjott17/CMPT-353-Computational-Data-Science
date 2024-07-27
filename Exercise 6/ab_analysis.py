import sys

import pandas as pd
from scipy import stats

OUTPUT_TEMPLATE = (
    '"Did more/less users use the search feature?" p-value:  {more_users_p:.3g}\n'
    '"Did users search more/less?" p-value:  {more_searches_p:.3g} \n'
    '"Did more/less instructors use the search feature?" p-value:  {more_instr_p:.3g}\n'
    '"Did instructors search more/less?" p-value:  {more_instr_searches_p:.3g}'
)


def main():
    searchdata_file = sys.argv[1]

    data = pd.read_json(searchdata_file, orient='records', lines=True)

    # reference from https://www.geeksforgeeks.org/how-to-filter-dataframe-rows-based-on-the-date-in-pandas/
    odd = data.loc[(data['uid'] % 2 == 1)]
    even = data.loc[(data['uid'] % 2 == 0)]

    odd_s = odd.loc[(odd['search_count'] > 0)]
    odd_search = odd_s['search_count']
    odd_no_s = odd.loc[(odd['search_count'] == 0)]
    odd_no_search = odd_no_s['search_count']
    even_s = even.loc[(even['search_count'] > 0)]
    even_search = even_s['search_count']
    even_no_s = even.loc[(even['search_count'] == 0)]
    even_no_search = even_no_s['search_count']

    contingency = [[len(odd_search), len(even_search)], [len(odd_no_search), len(even_no_search)]]
    chi2, chi_p, dof, expected = stats.chi2_contingency(contingency)

    u1, u_p = stats.mannwhitneyu(odd['search_count'], even['search_count'])

    ins_odd = odd.loc[(odd['is_instructor'] == True)]
    ins_odd_s = ins_odd.loc[(ins_odd['search_count'] > 0)]
    ins_odd_search = ins_odd_s['search_count']
    ins_no_odd_s = ins_odd.loc[(ins_odd['search_count'] == 0)]
    ins_no_odd_search = ins_no_odd_s['search_count']
    ins_even = even.loc[(even['is_instructor'] == True)]
    ins_even_s = ins_even.loc[(ins_even['search_count'] > 0)]
    ins_even_search = ins_even_s['search_count']
    ins_no_even_s = ins_even.loc[(ins_even['search_count'] == 0)]
    ins_no_even_search = ins_no_even_s['search_count']

    ins_contingency = [[len(ins_odd_search), len(ins_even_search)], [len(ins_no_odd_search), len(ins_no_even_search)]]
    ins_chi2, ins_p, ins_dof, ins_expected = stats.chi2_contingency(ins_contingency)

    ins_u1, ins_u_p = stats.mannwhitneyu(ins_odd['search_count'], ins_even['search_count'])

    # Output
    print(OUTPUT_TEMPLATE.format(
        more_users_p=chi_p,
        more_searches_p=u_p,
        more_instr_p=ins_p,
        more_instr_searches_p=ins_u_p,
    ))


if __name__ == '__main__':
    main()
