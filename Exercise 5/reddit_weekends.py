import sys

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from scipy import stats
from datetime import date


OUTPUT_TEMPLATE = (
    "Initial T-test p-value: {initial_ttest_p:.3g}\n"
    "Original data normality p-values: {initial_weekday_normality_p:.3g} {initial_weekend_normality_p:.3g}\n"
    "Original data equal-variance p-value: {initial_levene_p:.3g}\n"
    "Transformed data normality p-values: {transformed_weekday_normality_p:.3g} {transformed_weekend_normality_p:.3g}\n"
    "Transformed data equal-variance p-value: {transformed_levene_p:.3g}\n"
    "Weekly data normality p-values: {weekly_weekday_normality_p:.3g} {weekly_weekend_normality_p:.3g}\n"
    "Weekly data equal-variance p-value: {weekly_levene_p:.3g}\n"
    "Weekly T-test p-value: {weekly_ttest_p:.3g}\n"
    "Mann-Whitney U-test p-value: {utest_p:.3g}"
)


def main():
    reddit_counts = sys.argv[1]
    counts = pd.read_json(reddit_counts, lines=True)
    df = counts
    #reference from https://www.geeksforgeeks.org/how-to-filter-dataframe-rows-based-on-the-date-in-pandas/
    df = df.loc[(df['date'] >= '2012-01-01') & (df['date'] < '2014-01-01')]
    df = df.loc[(df['subreddit'] == 'canada')]

    #reference from https://stackoverflow.com/questions/48058304/how-to-apply-series-in-isocalendar-function-in-pandas-python
    df['date'] = pd.to_datetime(df['date'])
    df['year-week'] = df['date'].apply(lambda x: str(x.isocalendar()[0]) + '-' + str(x.isocalendar()[1]).zfill(2))

    #reference from #reference from https://www.geeksforgeeks.org/how-to-filter-dataframe-rows-based-on-the-date-in-pandas/
    df_weekdays = df.loc[(df['date'].dt.weekday >= 0) & (df['date'].dt.weekday <= 4)]
    df_weekends = df.loc[(df['date'].dt.weekday >= 5) & (df['date'].dt.weekday <= 6)]

    ttest = stats.ttest_ind(df_weekdays['comment_count'], df_weekends['comment_count'])
    weekday_normal = stats.normaltest(df_weekdays['comment_count']).pvalue
    weekend_normal = stats.normaltest(df_weekends['comment_count']).pvalue
    levene_original = stats.levene(df_weekdays['comment_count'], df_weekends['comment_count']).pvalue

    weekdays_transf = np.sqrt(df_weekdays['comment_count'])
    weekends_transf = np.sqrt(df_weekends['comment_count'])
    weekday_normal_transf = stats.normaltest(weekdays_transf).pvalue
    weekend_normal_transf = stats.normaltest(weekends_transf).pvalue
    levene_transf = stats.levene(weekdays_transf, weekends_transf).pvalue

    data_weekdays = df_weekdays.groupby(['year-week'])
    data_weekends = df_weekends.groupby(['year-week'])

    weekly_weekdays = data_weekdays['comment_count'].agg(np.mean)
    weekly_weekends = data_weekends['comment_count'].agg(np.mean)
    normal_weekly_weekdays = stats.normaltest(weekly_weekdays).pvalue
    normal_weekly_weekends = stats.normaltest(weekly_weekends).pvalue
    levene_weekly = stats.levene(weekly_weekdays, weekly_weekends).pvalue
    ttest_weekly = stats.ttest_ind(weekly_weekdays, weekly_weekends)

    mann_whitney = stats.mannwhitneyu(df_weekdays['comment_count'], df_weekends['comment_count']).pvalue

    print(OUTPUT_TEMPLATE.format(
        initial_ttest_p=ttest.pvalue,
        initial_weekday_normality_p=weekday_normal,
        initial_weekend_normality_p=weekend_normal,
        initial_levene_p=levene_original,
        transformed_weekday_normality_p=weekday_normal_transf,
        transformed_weekend_normality_p=weekend_normal_transf,
        transformed_levene_p=levene_transf,
        weekly_weekday_normality_p=normal_weekly_weekdays,
        weekly_weekend_normality_p=normal_weekly_weekends,
        weekly_levene_p=levene_weekly,
        weekly_ttest_p=ttest_weekly.pvalue,
        utest_p=mann_whitney,
    ))


if __name__ == '__main__':
    main()
