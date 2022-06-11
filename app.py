import pandas as pd
import requests
import json
from pandas.io.json import json_normalize
from functools import reduce
from datetime import datetime, timedelta
import openpyxl
import time
from time import mktime
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objs import *
from plotly.graph_objs.scatter.marker import Line
from plotly.subplots import make_subplots
import xlrd
import openpyxl
import numpy as np
import re
from bs4 import BeautifulSoup
import math
import plotly.io as pio
import plot_settings
from multiapp import MultiApp
import streamlit as st

# LOAD FILES
# summary - sorted list
# chgsort = pd.read_csv('list_of_ten_2020.csv', na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
# historical roic
roic_import = pd.read_csv('new_roic_historical_iwv_2016_2020_acq_nr.csv', low_memory=False,
                         na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
# beta
betadf = pd.read_csv('upside_downside_trailing_beta_iwv_2015_2019.csv', low_memory=False,
                    na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
# momentum
momo2021df = pd.read_csv('momentum_lookup_subtract_iwv_2016_2020.csv', low_memory=False,
                        na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
# prior year
prioryear = pd.read_csv('new_roic_historical_iwv_2020_acq_nr_one_day_offset.csv',
                        low_memory=False, na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
# beta2020
beta20df = pd.read_csv('upside_downside_trailing_beta_iwv_2020_20201231.csv',
                      low_memory=False, na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
# 2022 import
import_2022 = pd.read_csv('new_roic_historical_iwv_2021_acq_nr_20211231_dpz_fix.csv',
                         low_memory=False, na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
# beta2021
beta21df = pd.read_csv('upside_downside_trailing_beta_iwv_2021_20211231.csv',
                      low_memory=False, na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])

# CREATE ROIC_FILTERED
def create_roic_filtered(roic_import, betadf, momo2021df):
    # combine symbol and start date to create unique ID
    roic_import['Instance'] = roic_import['Symbol'].astype(str) + '_' + \
                              roic_import['StartDate'].astype(str)

    # set index to this unique ID
    roic_import = roic_import.set_index('Instance')

    # just keep one class of google shares
    roic_import = roic_import[roic_import['Symbol'] != 'GOOGL_US']

    roic_filtered = roic_import.fillna(0)

    # filter betadf to just the columns you want merged into roic_filtered
    betadf = betadf[['Instance', 'Beta', 'UpsideBeta', 'DownsideBeta']]

    # merge beta lookup into roic_filtered
    roic_filtered = roic_filtered.reset_index()
    roic_filtered = roic_filtered.merge(betadf, on='Instance', how='inner')

    roic_filtered['UpsideSpread'] = roic_filtered.UpsideBeta - roic_filtered.DownsideBeta
    roic_filtered = roic_filtered.drop_duplicates()

    # filter momo2021df to just the columns you want merged into roic_filtered
    momo2021df = momo2021df[['Instance', '3mo_Return', '6mo_Return', '1year_Return',
                             'relative_3mo_spy', 'relative_6mo_spy', 'relative_1y_spy',
                             'relative_3mo_ijr', 'relative_6mo_ijr', 'relative_1y_ijr',
                             'relative_3mo_xlp', 'relative_6mo_xlp', 'relative_1y_xlp']].fillna(0)

    # merge momo2021df into roic_filtered
    roic_filtered = roic_filtered.merge(momo2021df, on='Instance', how='inner'). \
        set_index('Instance')

    return roic_filtered


# CREATE FEATURES IN ROIC_FILTERED
def create_features(df):
    metric_types = ['EBIT', 'OCF', 'FCF']

    for t in metric_types:
        df[f'{t}_RD'] = df[t] + df.R_D
        df[f'{t}_RD_ROIC'] = df[f'{t}_RD'] / df.TangibleCapital
        df[f'{t}_RD_EV'] = df[f'{t}_RD'] / df.EnterpriseValue

    df = df.assign(Net_Cash_Pct=lambda t: t.Net_Cash / t.MarketCap,
                   RD_Cap=lambda t: t.R_D / t.TangibleCapital,
                   RD_Sales=lambda t: t.R_D / t.Sales,
                   Price_Sales=lambda t: t.MarketCap / t.Sales
                   )

    df = df.rename(columns={'R_D': 'RD'})

    # change any infinity or negative infinity values to NaN & fill NaNs with zeros
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)

    return df

# FORMAT PRIORYEAR
def format_prioryear(prioryear, beta20df):
    # filter to only 12/31/20 start dates
    prioryear = prioryear[prioryear['StartDate'] == '12/31/2020']

    # create the same unique ID column as in roic_filtered
    prioryear['Instance'] = prioryear['Symbol'].astype(str) + '_' + prioryear['StartDate']. \
        astype(str)

    # create features using the above function for ebit, ocf, fcf
    prioryear = create_features(prioryear)

    # filter beta20df to columns you want to merge into prioryear
    beta20df = beta20df[['Instance', 'Beta', 'UpsideBeta', 'DownsideBeta']]

    # merge beta20df into prioryear
    prioryear = prioryear.merge(beta20df, on='Instance', how='inner').set_index('Instance')

    return prioryear

# FORMAT 2022
def format_2022(import_2022, beta21df):
    # create the same unique ID column as in other dfs
    import_2022['Instance'] = import_2022['Symbol'].astype(str) + '_' + import_2022['StartDate']. \
        astype(str)

    # fill missing values with zeros
    import_2022 = import_2022.fillna(0)

    # create features using the above function for ebit, ocf, fcf
    import_2022 = create_features(import_2022)

    # filter beta21df to columns you want to merge into import_2022
    beta21df = beta21df[['Instance', 'Beta', 'UpsideBeta', 'DownsideBeta']].fillna(0)

    # merge beta21df into import_2022
    import_2022 = import_2022.merge(beta21df, on='Instance', how='inner').set_index('Instance')

    return import_2022

roic_filtered = create_roic_filtered(roic_import, betadf, momo2021df)
roic_filtered = create_features(roic_filtered)
prioryear = format_prioryear(prioryear, beta20df)
combined = pd.concat([roic_filtered, prioryear], sort=True).fillna(0)

import_2022 = format_2022(import_2022, beta21df)
combined = pd.concat([combined, import_2022], sort=True).reset_index()

cols_keep = ['Instance', 'Symbol', 'StartDate', 'Sales', 'EBIT', 'EBIT_ROIC', 'OCF',
             'OCF_ROIC', 'ROA', 'CurrentAssets', 'Cash', 'LT_Debt',
             'AccountsPayable', 'NetFixedAssets', 'TangibleCapital']

float_cols = cols_keep[3:]

df = combined[cols_keep]

for fc in float_cols:
    df[fc] = df[fc].map('{:,.2f}'.format)

def right_align(s, props='text-align: right;'):
    return props

print(df.columns)
print(df.info())

st.write(df.style.applymap(right_align))