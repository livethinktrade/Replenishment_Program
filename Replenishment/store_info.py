from psycopg2 import pool
import pandas as pd
import pandas.io.sql as psql


from Import.data_insertion import *
from Update.Transform_Sales_Data.transform import *
from Sales_Report.Reports.reports import *
from Sales_Report.Report_Format.weekly_toexcel import weekly_toexcel
from Sales_Report.Report_Format.ytd_toexcel import ytd_toexcel
from Sales_Report.Replenishment.replenishment import replenishment
from Sales_Report.Report_Format.weekly_sales_report_format import weekly_sales_report_format
from Sales_Report.Report_Format.ytd_sales_report_format import ytd_sales_report_format


class Replenishment():
    
    def __init__(self, store_type_input, transition_date_range, current_year, current_week):
        
        self.store_type_input = store_type_input
        
        self.connection_pool = pool.SimpleConnectionPool(1, 10000, 
                                            database= f"{self.store_type_input}", 
                                            user="postgres", 
                                            password="winwin", 
                                            host="localhost")

        self.connection = psycopg2.connect(database=f"{self.store_type_input}", user="postgres", password="winwin", host="localhost")

        self.current_year = current_year

        self.current_week = current_week

        self.transition_date_range = transition_date_range

        self.store_setting = pd.read_excel(rf'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                           sheet_name='Sheet2',
                                           header=None)

        self.store_programs = pd.read_excel(rf'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                            sheet_name='Store Programs')

        self.store_notes = pd.read_excel(rf'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                         sheet_name='Store Notes')

        self.master_planogram = pd.read_excel(rf'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Groccery Store Program\MASTER PLANOGRAM.xlsx',
                                         sheet_name='MASTER PLANOGRAM')


    def delivery_import(self, file):

        """This takes in an excel file and inserts delivery data into postgres
        Data collumns must be in this order

        (transition_year, transition_season, store_year, store_week, store_number,
        upc, sales, qty, current_year, current_week, store_type)"""

        new_deliv = pd.read_excel(f'{file}.xlsx')

        new_len = len(new_deliv)
        i = 0
        while i < new_len:
            transition_year = int(new_deliv.iloc[i, 0])
            transition_season = new_deliv.iloc[i, 1]
            ttype = new_deliv.iloc[i, 2]
            date = new_deliv.iloc[i, 3]
            upc = new_deliv.iloc[i, 4]
            store = new_deliv.iloc[i, 5]
            qty = new_deliv.iloc[i, 6]
            store_type = new_deliv.iloc[i, 7]
            # num = new_deliv.iloc[i, 8]
            # code = new_deliv.iloc[i,9]

            # note to self: last line was comment out bc i needed the old version of the deliv insert not the new one.

            delivery_insert(transition_year,
                            transition_season,
                            ttype,
                            date,
                            upc,
                            store,
                            qty,
                            store_type,
                            # num,
                            # code,
                            self.connection_pool)

            i += 1
        print('\nDelivery Data Imported')

    def delivery_update(self, file):

        '''Takes in csv file of delivery data from qb and converts it to proper formating so it can be
        imported into postgres'''

        connection = self.connection_pool.getconn()

        df = pd.read_csv(f'{file}.CSV')

        # drops uneccesary columns
        df = df.drop(columns=['Unnamed: 0', 'Item', 'U/M', 'Sales Price', 'Amount', 'Balance'])
        df = df.dropna()

        # filters out unnecesary qb types only invoice or credit memo
        invoice = df[df.Type == ('Invoice')]
        credits = df[df.Type == ('Credit Memo')]
        df = pd.concat([invoice, credits])
        df = df.sort_values(by=['Date'])

        # df1 = df['Memo'].str.split(n=1)
        df[['upc', 'memo']] = df.Memo.str.split('/', n=1, expand=True)
        df.pop('Memo')
        df.pop('memo')

        # adding transion column and renanimg columns
        df['transition_date_range'] = self.transition_date_range

        # takes hyphens out of up column extracts numbers only to prevent any rows that include freight or other non item sales (ie sales signs)
        df['upc'] = df["upc"].str.replace("-", "")
        df['upc'] = df.upc.str.extract('(\d+)')
        df = df.dropna()

        # filters out and rows that doesn't have 12 digits in the UPC column this will eliminate any data with POG in the orignal UPC column
        df['upc_len'] = df['upc'].str.len()
        df = df[df.upc_len == 12]
        df.pop('upc_len')

        # store numbers collumn added
        df['store'] = df.Name.str.extract('(\d+)')

        # getting the data to provide store_types collumn for database

        if self.store_type_input == 'kvat':
            df[['store_type', 'letter1']] = df.Name.str.split(':', n=1, expand=True)
            df.pop('letter1')

        elif self.store_type_input == 'fresh_encounter':
            df[['store_type', 'letter1']] = df.Name.str.split(',', n=1, expand=True)
            df.pop('letter1')
        else:

            df[['letter', 'store_type', 'letter1']] = df.Name.str.split(':', n=2, expand=True)
            df.pop('letter1')
            df.pop('letter')

        df.pop('Name')

        store_list = {
            'ACME MARKETS': 'acme',
            'JEWEL OSCO': 'jewel',
            'KROGER CENTRAL': 'kroger_central',
            'INTERMOUNTAIN DIVISION': 'intermountain',
            'KROGER COLUMBUS': 'kroger_columbus',
            'KROGER DALLAS': 'kroger_dallas',
            'KROGER DELTA': 'kroger_delta',
            'KROGER MICHIGAN': 'kroger_michigan',
            'ALBERTSONS DENVER': 'safeway_denver',
            'TEXAS DIVISION': 'texas_division',
            'KVAT FOOD STORES': 'kvat',
            'FRESH ENCOUNTER': 'fresh_encounter'

        }

        store_type = df.iloc[0, 7]

        df['store_type'] = store_list[store_type]

        # renaming collumns and putting them in the right order

        df = df.rename(columns={
            "Type": "type",
            "Date": "date",
            'Qty': 'qty',
            'Num': 'num'
        })

        new_deliv_transform = df[['transition_date_range', 'type', 'date', 'upc', 'store', 'qty', 'store_type', 'num']]

        ######LOADING DATA INTO POSTGRES WITH PYTHON #########

        new_deliv_transform_length = len(new_deliv_transform)
        i = 0
        update = 0
        insert = 0

        while i < new_deliv_transform_length:

            transition_date_range = new_deliv_transform.iloc[i, 0]
            type = new_deliv_transform.iloc[i, 1]
            date = new_deliv_transform.iloc[i, 2]
            upc = new_deliv_transform.iloc[i, 3]
            store = new_deliv_transform.iloc[i, 4]
            qty = new_deliv_transform.iloc[i, 5]
            store_type = new_deliv_transform.iloc[i, 6]
            num = new_deliv_transform.iloc[i, 7]

            duplicate_check = psql.read_sql(f"""
                                                SELECT * FROM DELIVERY 
                                                WHERE type ='{type}' and 
                                                    date = '{date}' and 
                                                    store = {store} and 
                                                    upc = '{upc}' and 
                                                    store_type = '{store_type}' and
                                                    num = {num}
                                            """, connection)

            duplicate_check_len = len(duplicate_check)

            if duplicate_check_len == 1:
                deliveryupdate(transition_date_range, type, date, upc, store, qty, store_type, self.connection_pool)
                update += 1
            else:
                delivery_insert(transition_date_range, type, date, upc, store, qty, store_type, num, self.connection_pool)
                insert += 1

            i += 1

            connection.commit()

        self.connection_pool.putconn(connection)

        print('\nDelivery Data Updated')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def sales_import(self, file):

        """'This takes in an excel file and inserts sales data into postgres
         Data collumns must be in this order

         (transition_year, transition_season, store_year, store_week, store_number,
         upc, sales, qty, current_year, current_week, store_type)"""

        new_sales = pd.read_excel(f'{file}.xlsx')

        new_len = len(new_sales)
        i = 0
        while i < new_len:
            transition_year = new_sales.iloc[i, 0]
            transition_season = new_sales.iloc[i, 1]
            store_year = new_sales.iloc[i, 2]
            store_week = new_sales.iloc[i, 3]
            store_number = new_sales.iloc[i, 4]
            upc = new_sales.iloc[i, 5]
            sales = new_sales.iloc[i, 6]
            qty = new_sales.iloc[i, 7]
            current_year = new_sales.iloc[i, 8]
            current_week = new_sales.iloc[i, 9]
            # store_type = new_sales.iloc[i, 10]

            sales_insert(transition_year, transition_season, store_year, store_week, store_number, upc, sales, qty,
                         current_year, current_week,
                         # store_type,
                         self.connection_pool)
            i += 1

        print('\nSales Data Imported')

    def sales_update(self, file):

        connection = self.connection_pool.getconn()

        kroger = [
                  'kroger_atlanta',
                  'kroger_central',
                  'kroger_cincinnati',
                  'kroger_columbus',
                  'kroger_dallas',
                  'kroger_delta',
                  'kroger_dillons',
                  'kroger_king_soopers',
                  'kroger_louisville',
                  'kroger_nashville',
                  'kroger_michigan']

        # series of if statement used to determine which program to use to transform the data to the proper format.

        if self.store_type_input in kroger:

            salesdata = kroger_transform(file, self.store_type_input, self.transition_date_range, self.current_year, self.current_week)

        elif self.store_type_input == 'kvat':

            salesdata = kvat_transform(file, self.transition_date_range, self.current_year, self.current_week)

        elif self.store_type_input == 'safeway_denver':

            salesdata = safeway_denver_transform(file, self.transition_date_range, self.current_year, self.current_week)

        elif self.store_type_input == 'jewel':

            salesdata = jewel_transform(file, self.transition_date_range, self.current_year, self.current_week, self.connection)

        # elif self.store_type_input == 'brookshire':
        #
        #     salesdata = brookshire_transform(file, self.transition_date_range, self.current_year, self.current_week)

        else:
            print('Update method is not established for this store')

        salesdata_len = len(salesdata)
        i = 0
        update = 0
        insert = 0
        inserted_list = []

        while i < salesdata_len:

            transition_date_range = salesdata.iloc[i, 0]
            store_year = salesdata.iloc[i, 1]
            store_week = salesdata.iloc[i, 2]
            store_number = salesdata.iloc[i, 3]
            upc = salesdata.iloc[i, 4]
            sales = salesdata.iloc[i, 5]
            qty = salesdata.iloc[i, 6]
            current_year = salesdata.iloc[i, 7]
            current_week = salesdata.iloc[i, 8]
            store_type = salesdata.iloc[i, 9]

            duplicate_check = psql.read_sql(f"""
                                                SELECT * FROM SALES 
                                                WHERE store_year ={store_year} and 
                                                    store_week = '{store_week}' and 
                                                    store_number = {store_number} and 
                                                    upc = '{upc}' and 
                                                    store_type = '{store_type}'
                                            """, connection)

            duplicate_check_len = len(duplicate_check)

            if duplicate_check_len == 1:

                salesupdate(transition_date_range,
                            store_year,
                            store_week,
                            store_number,
                            upc,
                            sales,
                            qty,
                            current_year,
                            current_week,
                            store_type,
                            self.connection_pool)
                update += 1
            else:
                sales_insert(transition_date_range,
                             store_year,
                             store_week,
                             store_number,
                             upc,
                             sales,
                             qty,
                             current_year,
                             current_week,
                             store_type,
                             self.connection_pool)
                insert += 1
                inserted_list.append(i)

            i += 1

            connection.commit()

        self.connection_pool.putconn(connection)

        print('\nSales Data Updated')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def case_capacity_import(self, file):

        case_capacity = pd.read_excel(f'{file}.xlsx')

        new_len = len(case_capacity)
        i = 0
        while i < new_len:
            store_type = case_capacity.iloc[i, 0]
            store = case_capacity.iloc[i, 1]
            rack_4w = case_capacity.iloc[i, 2]
            rack_1w = case_capacity.iloc[i, 3]
            rack_2w = case_capacity.iloc[i, 4]
            rack_pw = case_capacity.iloc[i, 5]
            carded = case_capacity.iloc[i, 6]
            long_hanging_top = case_capacity.iloc[i, 7]
            long_hanging_dress = case_capacity.iloc[i, 8]
            case_cap = case_capacity.iloc[i, 9]
            notes = case_capacity.iloc[i, 10]
            initial = case_capacity.iloc[i, 11]

            casecapacity_insert(store_type, store, rack_4w, rack_1w, rack_2w, rack_pw, carded, long_hanging_top,
                                long_hanging_dress, case_cap, notes, initial, self.connection_pool)

            i += 1

        print('\nCase Capacity Imported')

    def support_import(self, file):

        itemsupport = pd.read_excel(f'{file}.xlsx')

        item_support = len(itemsupport)
        i = 0

        while i < item_support:

            season = itemsupport.iloc[i, 0]
            category = itemsupport.iloc[i, 1]
            type = itemsupport.iloc[i, 2]
            style = itemsupport.iloc[i, 3]
            additional = itemsupport.iloc[i, 4]
            display_size = itemsupport.iloc[i, 5]
            pog_type = itemsupport.iloc[i, 6]
            upc = itemsupport.iloc[i, 7]
            code = itemsupport.iloc[i, 8]
            code_qb = itemsupport.iloc[i, 9]
            unique_replen_code = itemsupport.iloc[i, 10]
            case_size = itemsupport.iloc[i, 11]
            item_group_desc = itemsupport.iloc[i, 12]
            item_desc = itemsupport.iloc[i, 13]
            packing = itemsupport.iloc[i, 14]
            upc_11_digit = itemsupport.iloc[i, 15]

            item_support_insert(season,
                                category,
                                type,
                                style,
                                additional,
                                display_size,
                                pog_type,
                                upc,
                                code,
                                code_qb,
                                unique_replen_code,
                                case_size,
                                item_group_desc,
                                item_desc,
                                packing,
                                upc_11_digit,
                                self.connection_pool)
            i += 1

        print('\nSupport Sheet Imported')

    def sales_report(self):

        sales_data_fequency = {

            'jewel': 'weekly',
            'kvat': 'weekly',
            'fresh_encounter': 'weekly',

            'kroger_central': 'weekly',
            'kroger_columbus': 'weekly',
            'kroger_columbus': 'weekly',
            'kroger_dallas': 'weekly',
            'kroger_delta': 'weekly',
            'kroger_michigan': 'weekly',

            'brookshire': 'weekly',

            'safeway_denver': 'weekly',

            'texas_division': 'ytd',
            'acme': 'ytd',
            'intermountain': 'ytd'
        }

        replenishment_report = replenishment(self.store_type_input)

        replenishment_len = len(replenishment_report) + 1

        reports = Reports(self.store_type_input, self.store_setting)

        sales_report = reports.sales_table()
        item_sales_rank = reports.item_sales_rank()
        sales_sql_YTD_Mask = reports.ytd_table_mask()
        sales_sql_YTD_WoMask = reports.ytd_table_no_mask()
        sales_report_len = reports.sales_report_len(sales_report)
        on_hand = reports.on_hands()
        no_scan = reports.no_scan()

        if sales_data_fequency[self.store_type_input] == 'weekly':

            filename = weekly_toexcel(self.store_type_input,
                                      replenishment_report,
                                      sales_report,
                                      no_scan,
                                      sales_sql_YTD_Mask,
                                      sales_sql_YTD_WoMask,
                                      item_sales_rank,
                                      on_hand)

            weekly_sales_report_format(filename, replenishment_len, sales_report_len)

        elif sales_data_fequency[self.store_type_input] == 'ytd':
            filename = ytd_toexcel(self.store_type_input,
                                   replenishment_report,
                                   sales_report, no_scan,
                                   sales_sql_YTD_WoMask,
                                   item_sales_rank,
                                   on_hand)

            ytd_sales_report_format(filename, replenishment_len, sales_report_len)

        print("\nSales Report Generated")

    def store_import(self):

        new_len = len(self.store_notes)
        i = 0
        while i < new_len:

            store_id = self.store_notes.iloc[i, 0]
            initial = self.store_notes.iloc[i, 1]
            notes = self.store_notes.iloc[i, 2]

            store_insert(store_id, initial, notes, self.connection_pool)

            i += 1
        print('\n Store Data Imported')

    def store_program_import(self):

        new_len = len(self.store_programs)
        i = 0
        while i < new_len:

            store_id = self.store_programs.iloc[i, 0]
            program_id = self.store_programs.iloc[i, 1]
            activity = self.store_programs.iloc[i, 2]

            store_program_insert(store_id, program_id, activity, self.connection_pool)

            i += 1
        print('\n Store Program Data Imported')

    def master_planogram_import(self):

        new_len = len(self.master_planogram)
        i = 0
        while i < new_len:

            program_id = self.master_planogram.iloc[i, 0]
            carded = self.master_planogram.iloc[i, 1]
            long_hanging_top = self.master_planogram.iloc[i, 2]
            long_hanging_dress = self.master_planogram.iloc[i, 3]

            master_planogram_insert(program_id, carded, long_hanging_top, long_hanging_dress, self.connection_pool)

            i += 1
        print('\n Master Planogram Data Imported')






