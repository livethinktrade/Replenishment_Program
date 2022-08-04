# KROGER SALES DATA TRANSFORM transform df for aproprite store

import psycopg2
import pandas.io.sql as psql
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
import re
import pandas as pd
from datetime import datetime
import datetime as dt


def kroger_transform(file, store_type_input, transition_year, transition_season, current_year, current_week):
    # Read in new kroger sales data and then selects certain columns to a new df

    old = pd.read_csv(f'{file}', skiprows=16)
    new = old[
        ['RPT_SHORT_DESC', 'RE_STO_NUM', 'UPC', 'SCANNED_RETAIL_DOLLARS', 'SCANNED_MOVEMENT', 'MFR_DESC', 'WEEK_NAME']]

    # checking to see if items in sales report are only winwin products
    new_len = len(new)

    check = 'Pass'
    i = 0

    while i < new_len:
        MFR_check = new.loc[i, 'MFR_DESC']
        if MFR_check != 'WINWIN':
            check = "Fail"

        i += 1

    if check == "Pass":
        # Extracts out the data for the store year and and store week
        new[['store_year', 'name_of_code', 'cd', 'cd1', 'cd2', 'cd3', 'cd4', 'cd5', 'store_week']] = new[
            "WEEK_NAME"].str.split(" ", 8, expand=True)

        new = new[['RPT_SHORT_DESC',
                   'store_year',
                   'store_week',
                   'RE_STO_NUM',
                   'UPC',
                   'SCANNED_RETAIL_DOLLARS',
                   'SCANNED_MOVEMENT'
                   ]]

        # Takes the numbers out for the RPT_SHORT_DESC collumn leaving only the Divison name ie ('35 Dallas' => 'Dallas')
        new['store_week'] = new.store_week.str.extract('(\d+)')
        new['RPT_SHORT_DESC'] = new.RPT_SHORT_DESC.str.replace('[^a-zA-Z]', '')
        new['current_year'] = current_year
        new['current_week'] = current_week

        new['transition_year'] = f'{transition_year}'
        new['transition_season'] = f'{transition_season}'

        # Renames collumn names to approripate name for data insertion and update script
        new = new.rename(columns={
            'RPT_SHORT_DESC': 'store_type',
            'RE_STO_NUM': 'store_number',
            'UPC': 'upc',
            'SCANNED_RETAIL_DOLLARS': 'sales',
            'SCANNED_MOVEMENT': 'qty'
        })

        # reorganizes collumns to proper format for sales table
        new = new[['transition_year',
                   'transition_season',
                   'store_year',
                   'store_week',
                   'store_number',
                   'upc',
                   'sales',
                   'qty',
                   'current_year',
                   'current_week',
                   'store_type']]

        # converting division names into store_type for sales tables

        divisions = new.store_type.unique().tolist()
        division_len = len(divisions)

        store_list = {

            'Atl': 'kroger_atlanta',
            'Cincy': 'kroger_cincinatti',
            'Louisville': 'kroger_louisville',
            'Nashville': 'kroger_nashville',
            'Dillon': 'kroger_dillons',
            'KingSoopers': 'kroger_king_soopers',
            'Central': 'kroger_central',
            'Columbus': 'kroger_columbus',
            'Dallas': 'kroger_dallas',
            'Delta': 'kroger_delta',
            'Michigan': 'kroger_michigan'

        }

        # filter all of the data that pertains to the store_type_input
        store_type = store_type_input

        division = list(store_list.keys())[list(store_list.values()).index(store_type)]

        new_filt = new[new['store_type'] == division]

        #reset index and change store type
        new_filt = new_filt.reset_index(drop=True)
        new_filt['store_type'] = store_type
        salesdata = new_filt
    else:
        print('MFR_check failed. sales data sheet contains items that is not WinWin Products')
    return salesdata


def kvat_transform(file, transition_year, transition_season, current_year, current_week):
    # grabs the date that will be used to set the date across the board
    old = pd.read_excel(f'{file}', skiprows=1)
    date = old.iloc[0, 4]

    A = datetime.strptime(date, '%m/%d/%Y')
    store_year = A.year

    # this is finding the current week for the stores
    # day = A.day
    # month = A.month
    # year = A.year

    # current_week = date(A).isocalendar()[1]

    # recreating data frame so I don't have to redefine the column names
    old = pd.read_excel(f'{file}', skiprows=3)


    old['qty'] = old['Units Sold'] + old['Units Sold.1'] + old['Units Sold.2'] + old['Units Sold.3'] + old[
        'Units Sold.4'] + old['Units Sold.5'] + old['Units Sold.6']

    old['sales'] = old['Sales $'] + old['Sales $.1'] + old['Sales $.2'] + old['Sales $.3'] + old['Sales $.4'] + old[
        'Sales $.5'] + old['Sales $.6']

    # drops last row due to grand total. after dropping it,
    # store numbers can be then converted to int values

    old.drop(old.tail(1).index, inplace=True)  # drop last n rows

    old['store_number'] = old['Store Number'].astype(float).astype(np.int64)

    # upc values are converted from float to int64 (db requires this data type)

    old['UPC'] = old['UPC'].astype(float).astype(np.int64)

    # seting transition date range and store_week values for data and store_type
    old['transition_year'] = transition_year
    old['transition_season'] = f'{transition_season}'
    old['store_week'] = date
    old['store_type'] = 'kvat'
    old['store_year'] = store_year
    old['current_year'] = current_year
    old['current_week'] = current_week

    old = old.rename(columns={'UPC': 'upc'})

    salesdata = old[['transition_year',
                     'transition_season',
                     'store_year',
                     'store_week',
                     'store_number',
                     'upc',
                     'sales',
                     'qty',
                     'current_year',
                     'current_week',
                     'store_type'

                     ]]

    return salesdata


def safeway_denver_transform(file, transition_year, transition_season, current_year, current_week):
    # the first time reading the raw sales data file is extract the date from the first row

    old = pd.read_excel(f'{file}')

    names = old.columns
    string = names[0]

    # stripping date from string and converting it to datetime object

    match_str = re.search(r'\d{1}/\d{1}/\d{4}', string)

    if match_str == None:
        match_str = re.search(r'\d{1}/\d{2}/\d{4}', string)

    if match_str == None:
        match_str = re.search(r'\d{2}/\d{1}/\d{4}', string)

    if match_str == None:
        match_str = re.search(r'\d{2}/\d{2}/\d{4}', string)

    # computed date
    # feeding format
    date = datetime.strptime(match_str.group(), '%m/%d/%Y').date()

    # assign store year variable using datetime object
    store_year = date.year

    # assign store_week variable using datetime object
    store_week = date.strftime('%m/%d/%Y')


    # second time reading the raw file is to extract only the sales data

    old = pd.read_excel(f'{file}', skiprows=2)

    # select upc out of string and then converts to proper datatype for database
    old['UPC - Description'] = old['UPC - Description'].str[:11]
    old['UPC - Description'] = old['UPC - Description'].astype(str).astype(np.int64)

    old = old.rename(columns={'UPC - Description': 'upc',
                              'Sales (TY)': 'sales',
                              'Units (TY)': 'qty',
                              'Store': 'store_number'
                              })

    # creating transition collumn, store_week, store_year, store_type collumn

    old['transition_year'] = transition_year

    old['transition_season'] = f'{transition_season}'

    old['store_week'] = store_week

    old['store_year'] = store_year

    old['store_type'] = 'safeway_denver'

    old['current_year'] = current_year

    old['current_week'] = current_week

    salesdata = old[['transition_year',
                     'transition_season',
                     'store_year',
                     'store_week',
                     'store_number',
                     'upc',
                     'sales',
                     'qty',
                     'current_year',
                     'current_week',
                     'store_type']]

    return salesdata


def jewel_transform(file, transition_year, transition_season, current_year, current_week, connection, store_type_input):

    """Transform sales data  for jewel osco does not need the inputs of current_year or current_week"""

    old = pd.read_excel(f'{file}', sheet_name='Product Scan', skiprows=1)

    # verify data

    verify = old.loc[0, 'Division']
    if 'JEWEL' in verify:
        verify = 'pass'

    if verify == 'pass':
        # inserting neccessary collumns for sales insert function
        old['transition_year'] = transition_year
        old['transition_season'] = transition_season
        old['store_year'] = 0
        old['current_year'] = 0
        old['current_week'] = 0

        # Grabs only the neccessary collumns and reformats them into the neccesary df format for the sale insertert function
        old = old[['transition_year',
                   'transition_season',
                   'store_year',
                   'Day',
                   'Store',
                   'UPC',
                   'Sum Net Amount',
                   'Sum Item Quantity',
                   'current_year',
                   'current_week',
                   'Division']]

        old = old.rename(columns={
            'Division': 'store_type',
            'Store': 'store_number',
            'UPC': 'upc',
            'Sum Net Amount': 'sales',
            'Sum Item Quantity': 'qty',
            'Day': 'store_week'
        })

        # extracting only the store division name from colllumn and then lower caseing it so it can pass the db security check
        old['store_type'] = old.store_type.str.replace('[^a-zA-Z]', '')
        old['store_type'] = old.store_type.str.lower()

        # Find the last date in the db

        date = psql.read_sql(f'select max(store_week) from {store_type_input}.sales2', connection)
        date = pd.Timestamp(date.iloc[0, 0])

        # sorts dates in order
        old = old.sort_values(by='store_week', ascending=True)

        # using that date select the data from that date to present day in the sales sheet.
        filt = (old['store_week'] >= date)
        old = old.loc[filt]

        # assigns store year, current year, and current week using the store_week column.
        # Note that when finding the week number for the current week the week begins on Sunday and ends on Saturday
        # this is per the CVS Schedule

        old['store_year'] = old['store_week'].dt.year
        old['current_year'] = old['store_week'].dt.year
        old['current_week'] = old['store_week'].apply(lambda x: (x + dt.timedelta(days=1)).week)

        salesdata = old[['transition_year',
                         'transition_season',
                         'store_year',
                         'store_week',
                         'store_number',
                         'upc',
                         'sales',
                         'qty',
                         'current_year',
                         'current_week',
                         'store_type']]

        salesdata = salesdata.reset_index(drop=True)

    else:
        salesdata = "Failed"
        print("\n\nFAILED DATA VERIFICATION")

    return salesdata


def approval_transform(store_type_input, file):

    approval = pd.read_csv(f'{file}')

    store_price_column_name = {

        'kvat': 'KVAT Food Stores Price',
        'acme': 'Albertsons Acme Price',
        'brookshire': 'Brookshire Brothers Price',
        'kroger_nashville': 'Kroger - Nashville Price',
        'kroger_michigan': 'Kroger - Michigan Price',
        'kroger_louisville': 'Kroger South - Louisville Price',
        'kroger_king_soopers': 'Kroger - King Soopers Price',
        'kroger_delta': 'Kroger South - Delta Price',
        'kroger_dillons': 'Kroger - Dillons Price',
        'kroger_columbus': 'Kroger - Columbus Price',
        'kroger_cincinatti': 'Kroger - Cincinnati Price',
        'kroger_central': 'Kroger - Central Price',
        'kroger_dallas' : 'Kroger Texas - Dallas Price',
        'kroger_atlanta': 'Kroger South - Atlanta Price',
        'safeway_denver': 'Albertsons Denver Price',
        'jewel': 'Jewel Osco Price',
        'fresh_encounter': 'Fresh Encounter Price',
        'intermountain': 'Albertsons Intermountain Price',
        'texas_division': 'TX Safeway Albertsons Price',
        'follett' : 'Follett Price'

    }

    try:
        store_price_column_name = store_price_column_name[f'{store_type_input}']
        print()
        approval = approval[['Item', f'{store_price_column_name}']]

        approval = approval.rename(columns={f'{store_price_column_name}': 'store_price',
                                            'Item': 'code'})

        approval = approval.dropna()

    except:

        print(
            '\nStore not establisehd in transform approval function need to add store in the dictionary for the function\n')

    return approval


def inventory_transform(file):

    inventory = pd.read_csv(f'{file}')

    inventory = inventory[['Unnamed: 0', 'On Hand']]

    inventory = inventory.dropna()

    inventory[["Unnamed: 0", 'abc']] = inventory["Unnamed: 0"].str.split(" ", n=1, expand=True)

    inventory = inventory[['Unnamed: 0', 'On Hand']]

    inventory = inventory.rename(columns={'Unnamed: 0': 'code',
                                          'On Hand': 'on_hand'})

    return inventory
