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
from DbConfig import engine_pool_connection, PsycoPoolDB
import warnings
from Import.data_insertion import year_week_verify_insert



class TransformData():

    def __init__(self, store_type_input, transition_year, transition_season, connection):

        self.store_weeks_calender_file = r'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Replenishment program\Replenishment\support document\Store Weeks Calender.xlsx'

        self.winwin_calender = pd.read_excel(f'{self.store_weeks_calender_file}',
                                             sheet_name=f'WinWin Fiscal Year',
                                             names=('week_number', 'date', 'winwin year'))

        self.cvs_calender = pd.read_excel(f'{self.store_weeks_calender_file}',
                                          sheet_name=f'CVS Fiscal Year',
                                          names=('week_number', 'date', 'winwin year'))

        self.kroger_calender = pd.read_excel(f'{self.store_weeks_calender_file}',
                                             sheet_name=f'Kroger Fiscal Year',
                                             index_col= 0,
                                             names=('kroger_week_number', 'date', 'winwin_week_number', 'winwin year'))

        self.albertson_calender = pd.read_excel(f'{self.store_weeks_calender_file}',
                                                sheet_name=f'2022 Albertson Fiscal Year',
                                                names=('albertson_week_number', 'date', 'winwin_week_number', 'winwin year'))

        self.connection = connection

        self.store_type_input = store_type_input

        self.transition_year = transition_year

        self.transition_season = transition_season

    def kroger_transform(self, file):
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

            # taking the parenthesis out of data
            new['store_week'] = new.store_week.str.extract('(\d+)')

            # Takes the numbers out for the RPT_SHORT_DESC (store type) collumn leaving only the Divison name ie
            # ('35 Dallas' => 'Dallas')
            new['RPT_SHORT_DESC'] = new.RPT_SHORT_DESC.str.replace('[^a-zA-Z]', '')


            # Renames collumn names to approripate name for data insertion and update script
            new = new.rename(columns={
                'RPT_SHORT_DESC': 'store_type',
                'RE_STO_NUM': 'store_number',
                'UPC': 'upc',
                'SCANNED_RETAIL_DOLLARS': 'sales',
                'SCANNED_MOVEMENT': 'qty'
            })


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
            store_type = self.store_type_input

            division = list(store_list.keys())[list(store_list.values()).index(store_type)]

            new_filt = new[new['store_type'] == division]

            # reset index and change store type
            new_filt = new_filt.reset_index(drop=True)
            new_filt['store_type'] = store_type
            new = new_filt



            # adding the winwin year and weeknum column. weeknum column is added using the support document

            current_year = self.find_winwin_year('01-01-01')

            i = 0

            while i < len(new):

                store_week = int(new.loc[i, 'store_week'])

                store_number = new.loc[i, 'store_number']

                new.loc[i, 'current_week'] = int(self.kroger_calender.loc[store_week, 'winwin_week_number'])

                new.loc[i, 'date'] = self.kroger_calender.loc[store_week, 'date']

                new.loc[i, 'current_year'] = current_year

                upc_11_digit = str(new.loc[i, 'upc'])

                code = self.quickbooks_code_finder(upc_11_digit, store_number)

                new.loc[i, 'code'] = code

                i += 1

            new['transition_year'] = f'{self.transition_year}'
            new['transition_season'] = f'{self.transition_season}'

            # reorganizes columns to proper format for sales table
            sales_data = new[['transition_year',
                              'transition_season',
                              'store_year',
                              'date',
                              'store_week',
                              'store_number',
                              'upc',
                              'sales',
                              'qty',
                              'current_year',
                              'current_week',
                              'code',
                              'store_type']]

        else:
            raise Exception(' Error: MFR_check failed. sales data sheet contains items that is not WinWin Products')

        return sales_data

    def kvat_transform(self, file):

        # grabs the date that will be used to set the date across the board
        old = pd.read_excel(f'{file}', skiprows=1)
        date = old.iloc[0, 4]

        date = datetime.strptime(date, '%m/%d/%Y')
        store_year = date.year

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

        # upc values are converted from float to int64 (db requires this data type) & rename column

        old['UPC'] = old['UPC'].astype(float).astype(np.int64)
        old = old.rename(columns={'UPC': 'upc'})


        # seting transition date range and store_week values for data and store_type
        old['transition_year'] = self.transition_year
        old['transition_season'] = f'{self.transition_season}'
        old['date'] = date
        old['store_type'] = 'kvat'
        old['store_year'] = store_year
        old['current_year'] = self.find_winwin_year(date)

        week_num = self.date_to_week_number_conversion(date)
        old['current_week'] = week_num
        old['store_week'] = week_num

        i = 0

        while i < len(old):

            upc = old.loc[i, 'upc']
            store_number = old.loc[i, 'store_number']

            old.loc[i, 'code'] = self.quickbooks_code_finder(upc,store_number)

            i += 1

        salesdata = old[['transition_year',
                         'transition_season',
                         'store_year',
                         'date',
                         'store_week',
                         'store_number',
                         'upc',
                         'sales',
                         'qty',
                         'current_year',
                         'current_week',
                         'code',
                         'store_type'

                         ]]

        return salesdata

    def safeway_denver_transform(self, file):
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

        # creating transition column, store_week, store_year, store_type column

        old['transition_year'] = self.transition_year

        old['transition_season'] = f'{self.transition_season}'

        old['store_type'] = 'safeway_denver'

        old['date'] = date

        # find current store year and assigned to column
        # Important to note that Safeway Denver is on the WinWin Fiscal Year Calender

        current_year = self.find_winwin_year(date)
        old['store_year'] = current_year

        old['current_year'] = current_year


        #finds the current week number per Winwin calender
        current_week = self.date_to_week_number_conversion(date)

        old['current_week'] = current_week
        old['store_week'] = current_week

        i = 0

        while i < len(old):

            store_number = old.loc[i, 'store_number']
            upc_11_digit = old.loc[i, 'upc']
            old.loc[i, 'code'] = self.quickbooks_code_finder(upc_11_digit, store_number)
            i += 1

        sales_data = old[['transition_year',
                          'transition_season',
                          'store_year',
                          'date',
                          'store_week',
                          'store_number',
                          'upc',
                          'sales',
                          'qty',
                          'current_year',
                          'current_week',
                          'code',
                          'store_type']]

        return sales_data

    def jewel_transform(self, file):

        """Transform sales data  for jewel osco does not need the inputs of current_year or current_week"""

        old = pd.read_excel(f'{file}', sheet_name='Product Scan', skiprows=1)

        # verify data

        verify = old.loc[0, 'Division']
        if 'JEWEL' in verify:
            verify = 'pass'

        if verify == 'pass':
            # inserting necessary columns for sales insert function
            old['transition_year'] = self.transition_year
            old['transition_season'] = self.transition_season


            # Grabs only the necessary columns and reformats them into the necesary df format for the sale insert function
            old = old[['transition_year',
                       'transition_season',
                       'Day',
                       'Store',
                       'UPC',
                       'Sum Net Amount',
                       'Sum Item Quantity',
                       'Division']]

            old = old.rename(columns={
                'Division': 'store_type',
                'Store': 'store_number',
                'UPC': 'upc',
                'Sum Net Amount': 'sales',
                'Sum Item Quantity': 'qty',
                'Day': 'date'
            })

            # extracting only the store division name from column and then lower caseing it so it can pass the db security check
            old['store_type'] = old.store_type.str.replace('[^a-zA-Z]', '')
            old['store_type'] = old.store_type.str.lower()

            # Find the last date in the db

            date = psql.read_sql(f'select max(date) from {self.store_type_input}.sales2', self.connection)
            date = pd.Timestamp(date.iloc[0, 0])

            # sorts dates in order
            old = old.sort_values(by='date', ascending=True)

            # using that date select the data from that date to present day in the sales sheet.
            filter = (old['date'] >= date)
            old = old.loc[filter]

            # assigns store year, current year, and current week using the store_week column.
            # Note that when finding the week number for the current week the week begins on Sunday and ends on Saturday
            # this is per the CVS Schedule

            old = old.reset_index(drop=True)

            i = 0

            while i < len(old):

                upc_11_digit = old.loc[i, 'upc']
                store_number = old.loc[i, 'store_number']
                date = old.loc[i, 'date']

                week_number = int(self.date_to_week_number_conversion(date))

                year = self.find_winwin_year(date)

                old.loc[i, 'current_week'] = week_number

                old.loc[i, 'store_week'] = week_number

                old.loc[i, 'code'] = self.quickbooks_code_finder(upc_11_digit, store_number)

                old.loc[i, 'store_year'] = year

                old.loc[i, 'current_year'] = year

                i += 1

            old['current_week'] = old['current_week'].astype('int64')
            old['store_week'] = old['store_week'].astype('int64')
            old['store_year'] = old['store_year'].astype('int64')
            old['current_year'] = old['current_year'].astype('int64')

            sales_data = old[['transition_year',
                              'transition_season',
                              'store_year',
                              'date',
                              'store_week',
                              'store_number',
                              'upc',
                              'sales',
                              'qty',
                              'current_year',
                              'current_week',
                              'code',
                              'store_type']]

        else:

            raise Exception("\n\nFAILED DATA VERIFICATION")

        return sales_data

    def alba_transform(self, df):

        store_type_input_dict = {'acme': 'MID-ATLANTIC',
                                 'texas_division': 'SOUTHERN'}

        store_verify = store_type_input_dict[self.store_type_input]

        # dropping the last column where the total is located
        df.drop(index=df.index[-1],
                axis=0,
                inplace=True)

        # verify if data is all from the same division
        i = 0
        while i < len(df):

            division = df.loc[i, 'DIVISION']
            if division != store_verify:
                raise Exception(f"""
                Store Division Does not Match. Possible mixing of data
                Division should be {store_verify} but showing {division}
                on line {i + 1}""")
            i += 1

        start = (2020, 45)

        # establish variables to verify if the data is coming from the right year.
        start_verify = df.loc[2, '&GOLD']
        year_verify = int(start_verify[-6:-2])
        week_verify = int(start_verify[-2:])

        store_year_week = df.loc[3, '&GOLD']
        store_year = int(store_year_week[-6:-2])
        store_week = int(store_year_week[-2:])

        # verifying store year and week
        if year_verify != start[0]:
            raise Exception(f'Sales Data Does Not Start With The Same Year: currently at {year_verify}')

        if week_verify != start[1]:
            raise Exception(f"""

            Sales Data Does Not Start With The Same Week: 
            Alba Stores start with WK 45 week for file shows {week_verify}""")

        # filtering out UPC's that are not winwin products. The number that is used to be subtracted from the upc's
        # column is chosen because all of winwin products upc starts with the numbers 810312

        df['dif'] = df['UPC'] - 81031200000

        df = df[(df['dif'] <= 99999) & (df['dif'] >= 0)]

        df['store_year'] = store_year
        df['store_week'] = store_week
        date = self.alba_week_number_to_date(store_year, store_week)
        df['date'] = date

        cols = ['store_year', 'date', 'store_week', 'STORE', 'UPC', 'SALES TY', 'UNITS TY']

        filtered_columns = df[cols]

        # rename columns so it can be in the proper format to be used find_difference_bewtween_tables method
        filtered_columns = filtered_columns.rename(columns={
                                                            'STORE': 'store',
                                                            'UPC': 'upc',
                                                            'SALES TY': 'sales',
                                                            'UNITS TY': 'units'
                                                           })

        filtered_columns['sales'] = round(filtered_columns['sales'], 2)

        filtered_columns['upc'] = filtered_columns['upc'].astype(np.int64).astype(str)

        filtered_columns['store'] = filtered_columns['store'].astype(np.int64).astype(str)

        filtered_columns['store_type'] = self.store_type_input

        return filtered_columns

    def approval_transform(self, file):

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
            store_price_column_name = store_price_column_name[f'{self.store_type_input}']
            print()
            approval = approval[['Item', f'{store_price_column_name}']]

            approval = approval.rename(columns={f'{store_price_column_name}': 'store_price',
                                                'Item': 'code'})

            approval = approval.dropna()

        except:

            print(
                '\nStore not establisehd in transform approval function need to add store in the dictionary for the function\n')

        return approval

    def inventory_transform(self, file):

        inventory = pd.read_csv(f'{file}')

        inventory = inventory[['Unnamed: 0', 'On Hand']]

        inventory = inventory.dropna()

        inventory[["Unnamed: 0", 'abc']] = inventory["Unnamed: 0"].str.split(" ", n=1, expand=True)

        inventory = inventory[['Unnamed: 0', 'On Hand']]

        inventory = inventory.rename(columns={'Unnamed: 0': 'code',
                                              'On Hand': 'on_hand'})

        return inventory

    def date_to_week_number_conversion(self, date):
        """
        Takes in Dataframe

        note may be not relevant anymore. will need to review later.

        This is the reason why we are adding 8 days to the current day to find the weeknumber
        Pythons isocalender() function is used to calculate the week number and it has an atribute of week
        It considers Monday as the begining of the week however WinWin week starts on sunday so to address this problem
        I added one day to each date. The week number is still off by 1 so I then add another 7 days which is another week
        important to note that isocalendaer uses the gregorian calender which sometimes produces week 53 cvs does this as well

        """

        # assigns dt attributes to variable that way you can convert to np.datetime64
        # which is what dtype the winwincalender is

        year = date.year
        month = date.month
        day = date.day

        # for months and days that are < 10 they need to have a 0 in front of it otherwise they will not work when
        # forming the datetime64 type

        if month < 10:

            month = '0' + f'{month}'

        if day < 10:

            day = '0' + f'{day}'

        # creating the datetime64 data type
        search_date = np.datetime64(f'{year}-{month}-{day}')

        # using the search date to filter the winwin calender
        filter_date = self.winwin_calender[self.winwin_calender['date'] == search_date]

        if len(filter_date) < 1:
            raise Exception(f"""\nError: Need to update 'Store Weeks Calender' excel file.
            \t File: '{self.store_weeks_calender_file}'
            Update the WinWin Fiscal Year tab""")
        else:

            filter_date = filter_date.reset_index(drop=True)
            week_num = filter_date.loc[0, 'week_number']

        return week_num

    def quickbooks_code_finder(self, upc_11_digit, store):

        """Takes the 11 digit upc and finds the code by searching for the support sheet """

        try:

            code = f"""
            
            select delivery2.code from {self.store_type_input}.delivery2
            inner join public.item_support2 on {self.store_type_input}.delivery2.code = public.item_support2.code
            where  store = {store} and upc_11_digit = '{upc_11_digit}' order by date desc
            """

            code = psql.read_sql(code, self.connection)

            if len(code) > 0:

                code = code.iloc[0, 0]

            else:

                code = f"""

                select code
                from item_support2
                where upc_11_digit = '{upc_11_digit}'
                
                """

                code = psql.read_sql(code, self.connection)

                print(f'Found code for Sales table in support sheet instead of delivery table store {store} upc: {upc_11_digit}')

                code = code.iloc[0, 0]

        except Exception as e:

            raise Exception("ERROR : " + str(e))

        return code

    def find_winwin_year(self, date):

        kroger = [
                  'kroger_atlanta',
                  'kroger_central',
                  'kroger_cincinatti',
                  'kroger_columbus',
                  'kroger_dallas',
                  'kroger_delta',
                  'kroger_dillons',
                  'kroger_king_soopers',
                  'kroger_louisville',
                  'kroger_nashville',
                  'kroger_michigan']

        if self.store_type_input in kroger:
            date = dt.datetime.now()

        else:
            pass

        # assigns dt attributes to variable that way you can convert to np.datetime64
        # which is what dtype the winwincalender is

        year = date.year
        month = date.month
        day = date.day

        # for months and days that are < 10 they need to have a 0 in front of it otherwise they will not work when
        # forming the datetime64 type

        if month < 10:

            month = '0' + f'{month}'

        if day < 10:

            day = '0' + f'{day}'

        # creating the datetime64 data type
        search_date = np.datetime64(f'{year}-{month}-{day}')

        # using the search date to filter the winwin calender
        filter_date = self.winwin_calender[self.winwin_calender['date'] == search_date]

        if len(filter_date) < 1:
            raise Exception(f"""\nError: Need to update 'Store Weeks Calender' excel file.
            \t File: '{self.store_weeks_calender_file}'
            Update the WinWin Fiscal Year tab""")
        else:

            filter_date = filter_date.reset_index(drop=True)
            year = filter_date.loc[0, 'winwin year']

        return year

    def alba_week_number_to_date(self, store_year, store_week):

        calender_2021 = pd.read_excel((r'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Replenishment program\Replenishment\support document\Store Weeks Calender.xlsx'),
                                      sheet_name='2021 Albertson Fiscal Year')

        calender_2022 = pd.read_excel((r'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Replenishment program\Replenishment\support document\Store Weeks Calender.xlsx'),
                                      sheet_name='2022 Albertson Fiscal Year')

        date = None

        if store_year == 2021 and (store_week < 45):

            filtered_calender = calender_2021[calender_2021['Week Number'] == store_week]
            date = filtered_calender.iloc[0, 1]
            return date

        elif store_year == 2022 or (store_year == 2021 and (store_week <=52 and store_week >= 45)):

            filtered_calender = calender_2022[calender_2022['Week Number'] == store_week]
            date = filtered_calender.iloc[0, 1]
            return date
        else:
            return date

    def find_difference_between_tables(self, current_week_sales_data, previous_week_sales_data):
        
        """

        This function is used for raw sales data that is given for YTD Format.

        :param current_week_sales_data: dataframe
        :param previous_week_sales_data: dataframe
        :return: dataframe sales_data in the proper format for insertion into the designated division sales table

        """

        current_week_number = current_week_sales_data.loc[0, 'store_week']
        previous_week_number = previous_week_sales_data.loc[0, 'store_week']
        current_store_year = current_week_sales_data.loc[0, 'store_year']
        previous_store_year = previous_week_sales_data.loc[0, 'store_year']

        # verify data to see if the week difference is greater than one

        if current_store_year-previous_store_year == 0:

            if current_week_number < previous_week_number:
                raise Exception("""
                Data Verification Failed: "END WK" for current week is < than the previous week
                
                The store week number from the current week sales data is less the previous weeks sales data""")
            elif (current_week_number-previous_week_number) == 1:
                # pass verification test. Showing that the YTD sales data is 1 week apart.
                pass
            elif (current_week_number-previous_week_number) > 1:
                warnings.warn(f"""
                
                The 2 files from the sales data is showing that it is {(current_week_number-previous_week_number)} weeks
                apart.
                
                Ideally the sales data imported should be from week to week. However if more than a weeks of sales data
                was missed being sent, it is still ok to import the sales data.
                 
                If this is the case, enter 'VERIFIED' if not enter 'CANCEL'""")

                user_input = str(input('\n\n\nREAD STATEMENT ABOVE:\t')).upper()

                i = False

                while not i:

                    if user_input == 'VERIFIED':

                        i = True

                    elif user_input == 'CANCEL':
                        raise Exception("Import Canceled")

                    else:
                        user_input = str(input('READ STATEMENT ABOVE:\t')).upper()

            else:
                raise Exception('ERROR')

        elif current_store_year-previous_store_year == 1:
            pass
        else:
            raise Exception(f"""
            
            Data Verification Failed: Trying to import data from 2 different years. 
            
            Data from the first file is showing END WK: {current_store_year}
            Data form teh second file is showing END WK: {previous_store_year}""")

        # verifying to check if either of the weeks sales data has already been imported or the weeks in between the
        # current week or the previous week

        # check to see if the current week store year is in the system  or the weeks in between the previous week sales
        # data not done with this verification process. need to fix because the data will not check for multiple years

        # create a list of all the numbers in between that 2 sales date

        week_number_list = list(range(previous_week_number, current_week_number))

        year_week_list = []

        # creating the year week tuple list. list will be used later to check if the store year is in the db or not yet.

        for x in week_number_list:

            if (x >= 45 and x <= 53):
                year_week_list.append((previous_store_year, x))
            else:
                year_week_list.append((current_store_year, x))

        i = 0

        while i < len(year_week_list):

            year_week_verify = psql.read_sql(f"""select * from {self.store_type_input}.year_week_verify
                                                  where store_year = {year_week_list[i][0]} and
                                                        store_week = {year_week_list[i][1]} """, self.connection)

            # if the store_year and store_week is already in the table, then the data is already in the sales table.
            # passing this if statement would indicate that the data for the weeks in between is not in the sales table.

            if len(year_week_verify) >= 1:
                raise Exception(f'''
                Error: Import failed. 
                                    
                Tried to import duplication of same weeks. 
                Store Year:{year_week_list[i][0]}  Week:{year_week_list[i][1]}  sales data already in sales table''')
            else:
                pass

            i += 1

        # creating a primary key in the two tables that way we can do a left join on them. Will left join on the current
        # weeks data.
        
        current_week_sales_data['unique'] = current_week_sales_data['store'].astype(str) \
                                          + current_week_sales_data['upc'].astype(str)

        previous_week_sales_data['unique'] = previous_week_sales_data['store'].astype(str) \
                                           + previous_week_sales_data['upc'].astype(str)

        left_joined_sales_table = current_week_sales_data.merge(previous_week_sales_data, on='unique', how='left')
        
        # fill all data with na with the value of zero for the sales and units columns. necessary because sometimes the 
        # previous week that was join does not contain the primary key from the current week. possibility that a new 
        # item has been shipped.

        left_joined_sales_table = left_joined_sales_table.fillna(0)

        # find difference between the sales table
        left_joined_sales_table['sales'] = left_joined_sales_table['sales_x'] - left_joined_sales_table['sales_y']
        left_joined_sales_table['qty'] = left_joined_sales_table['units_x'] - left_joined_sales_table['units_y']

        # reorganizing columns & renaming
        column_names = left_joined_sales_table.columns.to_list()
        filtered_columns = column_names[:5] + column_names[-3:]
        left_joined_sales_table = left_joined_sales_table[filtered_columns]

        left_joined_sales_table = left_joined_sales_table.rename(columns={
                                                                          'store_year_x': 'store_year',
                                                                          'date_x': 'date',
                                                                          'store_week_x': 'store_week',
                                                                          'store_x': 'store_number',
                                                                          'upc_x': 'upc',
                                                                          'store_type_y': 'store_type'
                                                                         })

        # filtering all data that has 0 sales and units
        left_joined_sales_table = left_joined_sales_table[(left_joined_sales_table['sales'] != 0)
                                                          | (left_joined_sales_table['qty'] != 0)]

        left_joined_sales_table = left_joined_sales_table.reset_index(drop=True)

        # adding transition year, season, current year, current_week,
        left_joined_sales_table['transition_year'] = self.transition_year
        left_joined_sales_table['transition_season'] = f'{self.transition_season}'

        date = left_joined_sales_table.loc[0, 'date']
        left_joined_sales_table['current_year'] = self.find_winwin_year(date)
        left_joined_sales_table['current_week'] = self.date_to_week_number_conversion(date)

        left_joined_sales_table['store_type'] = self.store_type_input

        i = 0
        while i < len(left_joined_sales_table):

            store = left_joined_sales_table.loc[i, 'store_number']

            upc_11_digit = left_joined_sales_table.loc[i, 'upc']

            left_joined_sales_table.loc[i, 'code'] = self.quickbooks_code_finder(upc_11_digit, store)

            i += 1

        sales_data = left_joined_sales_table[['transition_year', 'transition_season', 'store_year', 'date',
                                              'store_week', 'store_number', 'upc', 'sales', 'qty',  'current_year',
                                              'current_week', 'code', 'store_type']]

        with PsycoPoolDB() as connection_pool:

            for x in year_week_list:

                store_year = x[0]
                store_week = x[1]
                year_week_verify_insert(store_year, store_week, self.store_type_input, connection_pool)

        return sales_data

    def ytd_transform(self, current_week_sales_data, previous_week_sales_data):

        current_week = pd.read_excel(current_week_sales_data)

        previous_week = pd.read_excel(previous_week_sales_data)

        if self.store_type_input in ['intermountain', 'acme', 'texas_division']:

            current_week_sales_data = self.alba_transform(current_week)

            previous_week_sales_data = self.alba_transform(previous_week)

            sales_data = self.find_difference_between_tables(current_week_sales_data, previous_week_sales_data)

        else:
            raise Exception(f"No YTD data pipeline established for {self.store_type_input}")

        return sales_data








