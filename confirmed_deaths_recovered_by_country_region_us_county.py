#!/usr/bin/env python
# coding: utf-8


import pandas as pd


def get_melt_prelim_df(confirmed_filename,
                       deaths_filename,
                       recovered_filename,
                       confirmed_id_date_idx,
                       deaths_id_date_idx,
                       recovered_id_date_idx):
    confirmed_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/" + "csse_covid_19_time_series/{confirmed_filename}".format(
        confirmed_filename=confirmed_filename)
    confirmed_df = pd.read_csv(confirmed_url)

    deaths_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/" + "csse_covid_19_time_series/{deaths_filename}".format(
        deaths_filename=deaths_filename)
    deaths_df = pd.read_csv(deaths_url)

    if recovered_filename:
        recovered_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/" + "csse_covid_19_time_series/{recovered_filename}".format(
            recovered_filename=recovered_filename)
        recovered_df = pd.read_csv(recovered_url)
    else:
        recovered_df = confirmed_df

    id_cols = confirmed_df.columns[0:confirmed_id_date_idx]
    date_cols = confirmed_df.columns[confirmed_id_date_idx:]

    confirmed_melt_prelim_df = pd.melt(confirmed_df,
                                       id_vars=id_cols,
                                       value_vars=date_cols,
                                       var_name='Date',
                                       value_name='Count')

    id_cols = deaths_df.columns[0:deaths_id_date_idx]
    date_cols = deaths_df.columns[deaths_id_date_idx:]

    deaths_melt_prelim_df = pd.melt(deaths_df,
                                    id_vars=id_cols,
                                    value_vars=date_cols,
                                    var_name='Date',
                                    value_name='Count')

    id_cols = recovered_df.columns[0:recovered_id_date_idx]
    date_cols = recovered_df.columns[recovered_id_date_idx:]

    if recovered_filename:
        recovered_melt_prelim_df = pd.melt(recovered_df,
                                           id_vars=id_cols,
                                           value_vars=date_cols,
                                           var_name='Date',
                                           value_name='Count')
    else:
        recovered_melt_prelim_df = confirmed_melt_prelim_df.copy(deep=True)
        recovered_melt_prelim_df['Count'] = 0

    confirmed_melt_prelim_df['Date'] = pd.to_datetime(confirmed_melt_prelim_df['Date'])
    deaths_melt_prelim_df.Date = pd.to_datetime(deaths_melt_prelim_df.Date)
    recovered_melt_prelim_df.Date = pd.to_datetime(recovered_melt_prelim_df.Date)

    confirmed_melt_prelim_df.insert(0, 'Measure', 'Confirmed')
    deaths_melt_prelim_df.insert(0, 'Measure', 'Deaths')
    recovered_melt_prelim_df.insert(0, 'Measure', 'Recovered')

    melt_prelim_df = confirmed_melt_prelim_df.append([deaths_melt_prelim_df, recovered_melt_prelim_df])
    melt_prelim_df.tail()

    return melt_prelim_df


def decom_recom(melt_prelim_df, group_by):
    melt_df = melt_prelim_df.loc[:, ('Measure', 'Date', *group_by, 'Count')]
    grouped_df = melt_df.groupby(['Measure', *group_by, 'Date'], as_index=False).sum()
    grouped_confirmed_df = grouped_df[grouped_df['Measure'] == 'Confirmed'].drop('Measure', axis=1)
    grouped_deaths_df = grouped_df[grouped_df['Measure'] == 'Deaths'].drop('Measure', axis=1)
    grouped_recovered_df = grouped_df[grouped_df['Measure'] == 'Recovered'].drop('Measure', axis=1)
    all_stats_df = pd.merge(grouped_confirmed_df, grouped_deaths_df, how='outer', on=[*group_by, 'Date'])
    all_stats_df = pd.merge(all_stats_df, grouped_recovered_df, how='outer', on=[*group_by, 'Date'])
    num_cols = len(all_stats_df.columns)
    all_stats_df = all_stats_df.rename(columns={all_stats_df.columns[num_cols - 3]: 'Confirmed',
                                                all_stats_df.columns[num_cols - 2]: 'Deaths',
                                                all_stats_df.columns[num_cols - 1]: 'Recovered'})
    return all_stats_df


melt_prelim_df = get_melt_prelim_df('time_series_covid19_confirmed_global.csv',
                                    'time_series_covid19_deaths_global.csv',
                                    'time_series_covid19_recovered_global.csv',
                                    4, 4, 4)
all_stats_df = decom_recom(melt_prelim_df, ['Country/Region'])
all_stats_df.to_csv('covid_by_country.csv', index=False)

melt_df = melt_prelim_df[melt_prelim_df['Province/State'].isnull() == False]
all_stats_df = decom_recom(melt_df, ['Country/Region', 'Province/State'])
all_stats_df.to_csv('covid_by_country_and_province.csv', index=False)

melt_prelim_df = get_melt_prelim_df('time_series_covid19_confirmed_US.csv',
                                    'time_series_covid19_deaths_US.csv',
                                    None,
                                    11, 12, 11)

all_stats_df = decom_recom(melt_prelim_df, ['Province_State', 'Admin2'])
all_stats_df.to_csv('covid_by_us_county.csv', index=False)
