import pandas as pd
import random
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

st.set_page_config(layout='wide')

# LOAD FILES
@st.cache
def load_files():
    # summary - sorted list
    # chgsort = pd.read_csv('list_of_ten_2020.csv', na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
    # historical roic
    roic_importL = pd.read_csv('new_roic_historical_iwv_2016_2020_acq_nr.csv', low_memory=False,
                             na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
    # beta
    betadfL = pd.read_csv('upside_downside_trailing_beta_iwv_2015_2019.csv', low_memory=False,
                        na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
    # momentum
    momo2021dfL = pd.read_csv('momentum_lookup_subtract_iwv_2016_2020.csv', low_memory=False,
                            na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
    # prior year
    prioryearL = pd.read_csv('new_roic_historical_iwv_2020_acq_nr_one_day_offset.csv',
                            low_memory=False, na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
    # beta2020
    beta20dfL = pd.read_csv('upside_downside_trailing_beta_iwv_2020_20201231.csv',
                          low_memory=False, na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
    # 2022 import
    import_2022L = pd.read_csv('new_roic_historical_iwv_2021_acq_nr_20211231_dpz_fix.csv',
                             low_memory=False, na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])
    # beta2021
    beta21dfL = pd.read_csv('upside_downside_trailing_beta_iwv_2021_20211231.csv',
                          low_memory=False, na_values=['#VALUE!', '#DIV/0!', '#SPILL!'])

    return roic_importL, betadfL, momo2021dfL, prioryearL, beta20dfL, import_2022L, beta21dfL

# CREATE ROIC_FILTERED
@st.cache
def format_dfs(roic_import, betadf, momo2021df, prioryear, beta20df, import_2022, beta21df):
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

    roic_filtered = create_features(roic_filtered)

    # format prioryear
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

    combined = pd.concat([roic_filtered, prioryear], sort=True).fillna(0)

    #format 2022
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

    combined = pd.concat([combined, import_2022], sort=True).reset_index()

    return roic_import, betadf, momo2021df, prioryear, beta20df, import_2022, beta21df, combined

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


roic_importL, betadfL, momo2021dfL, prioryearL, beta20dfL, import_2022L, beta21dfL = load_files()

roic_import = roic_importL.copy()
betadf = betadfL.copy()
momo2021df = momo2021dfL.copy()
prioryear = prioryearL.copy()
beta20df = beta20dfL.copy()
import_2022 = import_2022L.copy()
beta21df = beta21dfL.copy()

roic_import, betadf, momo2021df, prioryear, beta20df, import_2022, beta21df, combined = format_dfs(
roic_import, betadf, momo2021df, prioryear, beta20df, import_2022, beta21df
)


cols_keep = ['Instance', 'Symbol', 'StartDate', 'Sales', 'EBIT', 'EBIT_ROIC', 'OCF',
             'OCF_ROIC', 'ROA', 'CurrentAssets', 'Cash', 'LT_Debt',
             'AccountsPayable', 'NetFixedAssets', 'TangibleCapital']

float_cols = cols_keep[3:]

df = combined[cols_keep].sort_values(by=["Symbol", "StartDate"], ascending=[True, False]).reset_index(drop=True)

for fc in float_cols:
    df[fc] = df[fc].map('{:,.2f}'.format)

def right_align(s, props='text-align: right;'):
    return props

# st.write(df.style.applymap(right_align))
# st.write(df)

def shuffle(options):
    order = [x for x in range(1, len(options) + 1)]
    random.shuffle(order)

    compOrderDict = {}
    for i in range(len(options)):
        compOrderDict[order[i]] = options[i]

    return compOrderDict, order

def listToValues(lst):
    values = lst.join(', ')
    return values

def addToGood(goodList, index):
    goodList.append(index)
    return goodList

def blindPage():
    st.title('Blind Test Categorization')

    test_expander = st.expander("Possible Companies", expanded=True)
    with test_expander:
        companyOptions = sorted(df.Symbol.unique())

        options = st.multiselect(label="Companies Included in Blind Test",
                                 options=companyOptions,
                                 default=["AAPL-US", "MSFT-US", "GOOG-US"])

    compOrderDict, order = shuffle(options)

    testdf = df[df.Symbol.isin(options)]

    box1, s1, box2, s2, box3 = st.columns((1,.02,1,.02,1))

    goodList = []
    badList = []
    unsureList = []

    goodCompanies = box1.text_area(label="Good Companies",
                                   value=goodList,
                                   on_change=listToValues,
                                   args=(goodList),
                                   placeholder="None")

    unsureCompanies = box1.text_area(label="Unsure Companies",
                                   value=unsureList,
                                   on_change=listToValues,
                                   args=unsureList,
                                   placeholder="None")

    badCompanies = box1.text_area(label="Bad Companies",
                                   value=badList,
                                   on_change=listToValues,
                                   args=badList,
                                   placeholder="None")

    col1, sp1, col2, sp2, col3, sp3, col4 = st.columns((1,.02,1,.02,1,.02,1))
    index = col1.number_input(label="Company #",
                            min_value=min(order),
                            max_value=max(order),
                            value=1)

    addGood = col2.button(label="Good Company", help="Add company to 'Good' list",
                          on_click=addToGood, args=(goodList,index))

    reduce_cols = ['StartDate', 'Sales', 'EBIT', 'EBIT_ROIC', 'OCF',
             'OCF_ROIC', 'ROA', 'CurrentAssets', 'Cash', 'LT_Debt',
             'AccountsPayable', 'NetFixedAssets', 'TangibleCapital']

    compdf = testdf[testdf.Symbol==compOrderDict[index]][reduce_cols].set_index("StartDate")

    st.write(compdf.style.applymap(right_align))


    # st.write(testdf)


def create_app_with_pages():
    # CREATE PAGES IN APP
    app = MultiApp()
    app.add_app("Blind Test", blindPage, [])
    # app.add_app("Call & Put Volumes", callput_page, [])
    app.run(logo_path='logo.png')

if __name__ == '__main__':
    create_app_with_pages()