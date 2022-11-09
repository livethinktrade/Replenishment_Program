import pandas as pd

from Import.data_insertion import *
from Update.Transform_Sales_Data.transform import *
from Update.Transform_Sales_Data.history_tracking import *
from Sales_Report.Reports.reports import *
from Sales_Report.Replenishment.replenishment import *
from store_list import *
from datetime import datetime
import DbConfig


class Replenishment:

    def __init__(self, store_type_input):
        
        self.store_type_input = store_type_input
        
        self.connection_pool = DbConfig.create_pool_dbconnection()

        self.connection = DbConfig.engine_pool_connection()

        self.store_setting = pd.read_excel(rf'C:\Users\User1\OneDrive - winwinproducts.com\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                           sheet_name='Sheet2',
                                           header=None,
                                           index_col=0,
                                           names=('setting', 'values'))

        self.transition_year = self.store_setting.loc['Transition_year', 'values']
        self.transition_season = self.store_setting.loc['Transition_Season', 'values']

        self.store_programs = pd.read_excel(rf'C:\Users\User1\OneDrive - winwinproducts.com\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                            sheet_name='Store Programs')

        self.store_notes = pd.read_excel(rf'C:\Users\User1\OneDrive - winwinproducts.com\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                         sheet_name='Store Notes')

        self.master_planogram = pd.read_excel(r'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Replenishment program\Replenishment\support document\MASTER PLANOGRAM.xlsx',
                                              sheet_name='MASTER PLANOGRAM UPDATED', skiprows=1, )

        self.data_locker = DataLocker(self.store_type_input)

    def delivery_import(self, file):

        """This takes in an excel file and inserts delivery data into postgres
        Data collumns must be in this order

        (transition_year, transition_season, store_year, store_week, store_number,
        upc, sales, qty, current_year, current_week, store_type)"""

        new_deliv = pd.read_excel(f'{file}')

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
            num = new_deliv.iloc[i, 8]
            code = new_deliv.iloc[i, 9]

            if store_type != self.store_type_input:
                raise Exception(f'Data Validation Failed Inserted {store_type} for {self.store_type_input} database')

            # note to self: last line was comment out bc i needed the old version of the deliv insert not the new one.

            delivery_insert(transition_year,
                            transition_season,
                            ttype,
                            date,
                            upc,
                            store,
                            qty,
                            store_type,
                            num,
                            code,
                            self.connection_pool,
                            self.store_type_input)

            i += 1

        print(f'\n {self.store_type_input} Delivery Data Imported')

    def delivery_update(self, file):

        """Takes in csv file of delivery data from qb and converts it to proper formating so it can be
        imported into postgres"""

        df = pd.read_csv(f'{file}')

        # drops unnecessary columns
        df = df.drop(columns=['Unnamed: 0', 'U/M', 'Sales Price', 'Amount', 'Balance'])
        df = df.dropna()

        # filters out unnecessary qb types only invoice or credit memo
        invoice = df[df.Type == 'Invoice']
        credits = df[df.Type == 'Credit Memo']
        df = pd.concat([invoice, credits])
        df = df.sort_values(by=['Date'])

        # splitting memo to get the upc
        df[['upc', 'memo']] = df.Memo.str.split('/', n=1, expand=True)
        df.pop('Memo')
        df.pop('memo')

        # splitting items to get the qb code child class
        df[['code', 'junk']] = df.Item.str.split(' ', n=1, expand=True)
        df.pop('junk')
        df.pop('Item')

        # adding transion column and renanimg columns
        df['transition_year'] = self.transition_year
        df['transition_season'] = self.transition_season

        # takes hyphens out of up column extracts numbers only to prevent any rows that include
        # freight or other non-item sales (ie sales signs)
        df['upc'] = df["upc"].str.replace("-", "")
        df['upc'] = df.upc.str.extract('(\d+)')
        df = df.dropna()

        # filters out and rows that doesn't have 12 digits in the UPC column this will
        # eliminate any data with POG in the orignal UPC column
        df['upc_len'] = df['upc'].str.len()
        df = df[df.upc_len == 12]
        df.pop('upc_len')

        # store numbers collumn added
        df['store'] = df.Name.str.extract('(\d+)')

        # getting the data to provide store_types collumn for database

        if self.store_type_input == 'kvat':
            df[['store_type', 'letter1']] = df.Name.str.split(':', n=1, expand=True)
            df.pop('letter1')

        elif self.store_type_input in ['sal', 'midwest']:
            df[['store_type', 'letter1']] = df.Name.str.split(',', n=1, expand=True)
            df.pop('letter1')

        elif self.store_type_input == 'follett':
            df[['store_type', 'letter1']] = df.Name.str.split(':', n=1, expand=True)
            df.pop('letter1')

        else:

            df[['letter', 'store_type', 'letter1']] = df.Name.str.split(':', n=2, expand=True)
            df.pop('letter1')
            df.pop('letter')

        df.pop('Name')

        delivery_name = df.iloc[0, 9]

        try:
            store_list_filtered = store_list[(store_list['Delivery Name'] == delivery_name)]

            store_list_filtered = store_list_filtered.reset_index(drop=True)

            store_type = store_list_filtered.loc[0, 'store_type_input']

        except KeyError:
            raise Exception("Update Store List File in the Support Documents Folder. Add new stores if needed")

        df['store_type'] = store_type

        # renaming columns and putting them in the right order

        df = df.rename(columns={
            "Type": "type",
            "Date": "date",
            'Qty': 'qty',
            'Num': 'num'
        })

        # create new column by reorganizing column and resetting index
        new_deliv_transform = df[
            ['transition_year', 'transition_season', 'type', 'date', 'upc', 'store', 'qty', 'store_type', 'num',
             'code']]

        """
        Separating the data from fresh encounter. since delivery data for fresh encounter contains both SAL and midwest
        delivery data. it is important to separate the data and make sure it is being inserted into it assigned 
        delivery table.

        The two stores are separated because the SAL division is in the SS season all year while the midwest division 
        switches seasons as the  year progresses. 
        """

        new_deliv_transform['store'] = new_deliv_transform['store'].astype(int)

        if self.store_type_input == 'sal':

            new_deliv_transform = new_deliv_transform[(new_deliv_transform['store'] >= 400) &
                                                      (new_deliv_transform['store'] <= 499)]

            new_deliv_transform['store_type'] = self.store_type_input

        elif self.store_type_input == 'midwest':

            new_deliv_transform = new_deliv_transform[(new_deliv_transform['store'] < 400) |
                                                      (new_deliv_transform['store'] > 499)]

            new_deliv_transform['store_type'] = self.store_type_input

        else:
            pass

        new_deliv_transform = new_deliv_transform.reset_index(drop=True)
        new_deliv_transform['store'] = new_deliv_transform['store'].astype(str)

        """ 
        Transition Setting Verification
        This section is here to make sure each item being inserted into the table is
        contains the correct transition settings
        """

        verification = new_deliv_transform.copy()
        verification[['code', 'season']] = verification.code.str.split('-', n=1, expand=True)

        if self.transition_season == 'SS' and self.store_setting.loc['SS_Season', 'values'] == 1:

            check = verification[(verification['type'] == 'Invoice') & (verification['season'] == 'FW')]

            if len(check) >= 1:
                raise Exception("""
                FW Delivery Item is trying to be inserted into the transition_season column under "SS" transition setting
                Items that are Invoices and are FW should be inserted with "FW" for the transition_season column
                
                To fix this switch the transition setting to FW_season or Rolling_SS_FW and set transition_season to 'FW'
                
                This is important because the on hands program filters based off of the transition_year and 
                transition_season columns. If not set properly, some items will not make it to the on hands
                """)

        elif self.transition_season == "FW" and self.store_setting.loc['FW_Season', 'values'] == 1:

            check = verification[(verification['type'] == 'Invoice') & (verification['season'] == 'SS')]

            if len(check) >= 1:
                raise Exception("""
                SS Delivery Item is trying to be inserted into the transition_season column under "FW" transition setting
                Items that are Invoices and are SS should be inserted with "SS" for the transition_season column

                To fix this switch the transition setting to SS_season or Rolling_FW_SS and set transition_season to 'SS'

                This is important because the on hands program filters based off of the transition_year and 
                transition_season columns. If not set properly, some items will not make it to the on hands  
                """)

        # will add warning for the next 2 elifs down the road. Potential risk of selecting the wrong setting.
        elif self.transition_season == 'FW' and self.store_setting.loc['Rolling_SS_FW', 'values'] == 1:
            pass

        elif self.transition_season == 'SS' and self.store_setting.loc['Rolling_FW_SS', 'values'] == 1:
            pass

        else:
            raise Exception(f"Error: transition Season should only be SS or FW. Currently Set to {self.transition_season}")

        # Refresh Program (Band-Aid & Credit Memo duplication prevention program)

        bandaid = StoreRefresh(self.data_locker)
        bandaid.bandaid_adjustments_duplication_prevention(new_deliv_transform)
        bandaid_adjustments = bandaid.new_bandaid_adjustments

        # Loading Data into DB

        i = 0
        update = 0
        insert = 0

        with DbConfig.EnginePoolDB() as connection:

            while i < len(new_deliv_transform):

                transition_year = new_deliv_transform.loc[i, 'transition_year']

                transition_season = new_deliv_transform.loc[i, 'transition_season']

                type = new_deliv_transform.loc[i, 'type']

                date = new_deliv_transform.loc[i, 'date']

                upc = new_deliv_transform.loc[i, 'upc']

                store = new_deliv_transform.loc[i, 'store']

                qty = new_deliv_transform.loc[i, 'qty']

                store_type = new_deliv_transform.loc[i, 'store_type']

                num = new_deliv_transform.loc[i, 'num']

                code = new_deliv_transform.loc[i, 'code']

                duplicate_check = psql.read_sql(f"""
                                                    SELECT * FROM {self.store_type_input}.DELIVERY2 
                                                    WHERE type ='{type}' and 
                                                            date = '{date}' and 
                                                            store = {store} and 
                                                            upc = '{upc}' and 
                                                            store_type = '{store_type}' and
                                                            code = '{code}' and
                                                            num = '{num}'
                                                """, connection)

                duplicate_check_len = len(duplicate_check)

                if duplicate_check_len == 1:
                    delivery_update(transition_year, transition_season, type, date,
                                    upc, store, qty, store_type, num, code, self.connection_pool, self.store_type_input)
                    update += 1
                else:
                    delivery_insert(transition_year,
                                    transition_season,
                                    type, date, upc, store, qty, store_type,
                                    num, code, self.connection_pool, self.store_type_input)
                    insert += 1

                i += 1

            bandaid = 0
            x = 0
            while x < len(bandaid_adjustments):

                delivery_type = bandaid_adjustments.loc[x, 'type']
                store_id = bandaid_adjustments.loc[x, 'store_id']
                item_group_desc = bandaid_adjustments.loc[x, 'item_group_desc']
                qty = bandaid_adjustments.loc[x, 'qty']
                date_created = bandaid_adjustments.loc[x, 'date_created']
                effective_date = bandaid_adjustments.loc[x, 'effective_date']
                store_type = bandaid_adjustments.loc[x, 'store_type']
                reason = bandaid_adjustments.loc[x, 'reason']

                bandaid_insert(delivery_type, store_id, item_group_desc, qty,
                               date_created, effective_date, store_type, reason,
                               self.connection_pool, self.store_type_input)

                bandaid += 1
                x += 1

        print(f'\n {self.store_type_input} Delivery Data Updated')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')
        print('Bandaids:', bandaid, 'Records')

    def sales_import(self, file):

        """'This takes in an excel file and inserts sales data into postgres
         Data columns must be in this order

         (transition_year, transition_season, store_year, store_week, store_number,
         upc, sales, qty, current_year, current_week, store_type)"""

        new_sales = pd.read_excel(f'{file}')

        new_len = len(new_sales)
        i = 0
        while i < new_len:
            transition_year = new_sales.loc[i, 'transition_year']
            transition_season = new_sales.loc[i, 'transition_season']
            store_year = new_sales.loc[i, 'store_year']
            date = new_sales.loc[i, 'date']
            store_week = new_sales.loc[i, 'store_week']
            store_number = new_sales.loc[i, 'store_number']
            upc = new_sales.loc[i, 'upc']
            sales = new_sales.loc[i, 'sales']
            qty = new_sales.loc[i, 'qty']
            current_year = new_sales.loc[i, 'current_year']
            current_week = new_sales.loc[i, 'current_week']
            code = new_sales.loc[i, 'code']
            store_type = new_sales.loc[i, 'store_type']

            if store_type != self.store_type_input:
                raise Exception(f'Data Validation Failed Inserted data for {store_type} for {self.store_type_input} database')

            sales_insert(transition_year, transition_season, store_year, date,
                         store_week, store_number, upc, sales,
                         qty, current_year, current_week, code, store_type,
                         self.connection_pool, self.store_type_input)
            i += 1

        print(f'\n {self.store_type_input} Sales Data Imported')

    def sales_update(self, current_weeks_sales=None, previous_weeks_sales=None):

        '''

        Updates the sales tables for the designated grocery store.
        2 parameter are needed for stores that are in the YTD formart. ie
            (alba stores, follett, and the old format of fresh Encounter data)

        current_weeks_sales: name of the csv or excel needed for all Stores
        previous_weeks_sales: name of the csv or excel file. This parameter is only necessary for stores that are YTD

        return: nothing

        imports the data into the Postgres Database

        '''

        with DbConfig.EnginePoolDB() as connection:

            transform = TransformData(self.store_type_input, self.transition_year, self.transition_season, connection)

        # series of if statement used to determine which program to use to transform the data to the proper format.

        if self.store_type_input in kroger_stores:

            sales_data = transform.kroger_transform(current_weeks_sales)

        elif self.store_type_input == 'kvat':

            sales_data = transform.kvat_transform(current_weeks_sales)

        elif self.store_type_input == 'safeway_denver':

            sales_data = transform.safeway_denver_transform(current_weeks_sales)

        elif self.store_type_input == 'jewel':

            sales_data = transform.jewel_transform(current_weeks_sales)
        
        elif self.store_type_input in albertson_division:
            
            sales_data = transform.ytd_transform(current_weeks_sales, previous_weeks_sales)

        elif self.store_type_input in fresh_encounter:

            sales_data = transform.fresh_encounter_transform(current_weeks_sales)

        else:
            raise Exception('Error: Sales update method is not established for this store')

        i = 0
        update = 0
        insert = 0
        inserted_list = []

        with DbConfig.PsycoPoolDB() as connection_pool:

            with DbConfig.EnginePoolDB() as engine:

                while i < len(sales_data):

                    transition_year = sales_data.loc[i, 'transition_year']
                    transition_season = sales_data.loc[i, 'transition_season']
                    store_year = sales_data.loc[i, 'store_year']
                    date = sales_data.loc[i, 'date']
                    store_week = sales_data.loc[i, 'store_week']
                    store_number = sales_data.loc[i, 'store_number']
                    upc = sales_data.loc[i, 'upc']
                    sales = sales_data.loc[i, 'sales']
                    qty = sales_data.loc[i, 'qty']
                    current_year = sales_data.loc[i, 'current_year']
                    current_week = sales_data.loc[i, 'current_week']
                    code = sales_data.loc[i, 'code']
                    store_type = sales_data.loc[i, 'store_type']

                    # verifying data to make sure correct data is being inserted into the correct database
                    if store_type == self.store_type_input:
                        pass
                    else:
                        raise Exception(f'Error: {store_type} is trying to be inserted into the {self.store_type_input} table')

                    # Decided to assign query to variable and then plug into the read_sql method for debugging purposes
                    duplicate_check_query = f"""

                    SELECT * FROM {self.store_type_input}.SALES2 
                    WHERE store_year ={store_year} and 
                        store_week = '{store_week}' and
                        date = '{date}' and 
                        store_number = {store_number} and 
                        upc = '{upc}' and 
                        store_type = '{store_type}'
                    
                    """

                    duplicate_check = psql.read_sql(duplicate_check_query, engine)

                    duplicate_check_len = len(duplicate_check)

                    # Note to future developer
                    '''
                    When searching to see if the data has already been in the dataset or not. 
                    The length of the output of the query (ie the variable duplicate_check)
                    Should either be 1 or 0. 
                    
                    1 meaning that the data is already in the db and 0 for it is not in the db
                    
                    The length of the query should never be > 1. If so then most likely you have
                    either done one of the following:
                    
                        1) Entered the same record into the db more than once.
                        2) Did not aggregate the data by week or day depending on the sales data.
                          
                    '''

                    if duplicate_check_len == 1:

                        salesupdate(transition_year,
                                    transition_season,
                                    store_year,
                                    date,
                                    store_week,
                                    store_number,
                                    upc,
                                    sales,
                                    qty,
                                    current_year,
                                    current_week,
                                    code,
                                    store_type,
                                    connection_pool,
                                    self.store_type_input)
                        update += 1

                    elif duplicate_check_len == 0:
                        sales_insert(transition_year,
                                     transition_season,
                                     store_year,
                                     date,
                                     store_week,
                                     store_number,
                                     upc,
                                     sales,
                                     qty,
                                     current_year,
                                     current_week,
                                     code,
                                     store_type,
                                     connection_pool,
                                     self.store_type_input)

                        insert += 1
                        inserted_list.append(i)

                    elif duplicate_check_len > 1:

                        raise Exception(f'''
                        
                        Error: Duplicate data error.
                        
                        Potential of duplicate data risk. Need to investigate. 
                        
                        Use this query to look into the data:
                        
                        {duplicate_check_query}
                        
                        ''')

                    else:
                        raise Exception("Error: This is not suppose to happened")

                    i += 1

        print(f'\n {self.store_type_input} Sales Data Updated')
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

        print(f'\n {self.store_type_input} Case Capacity Imported')

    def support_import(self, file):

        itemsupport = pd.read_excel(f'{file}')

        update = 0
        insert = 0

        item_support = len(itemsupport)
        i = 0

        with DbConfig.EnginePoolDB() as connection_pool:

            while i < item_support:

                season = itemsupport.loc[i, 'season']
                category = itemsupport.loc[i, 'category']
                type = itemsupport.loc[i, 'type']
                style = itemsupport.loc[i, 'style']
                additional = itemsupport.loc[i, 'additional']
                display_size = itemsupport.loc[i, 'display_size']
                pog_type = itemsupport.loc[i, 'pog_type']
                upc = itemsupport.loc[i, 'upc']
                code = itemsupport.loc[i, 'code']
                code_qb = itemsupport.loc[i, 'code_qb']
                unique_replen_code = itemsupport.loc[i, 'unique_replen_code']
                case_size = itemsupport.loc[i, 'case_size']
                item_group_desc = itemsupport.loc[i, 'item_group_desc']
                item_desc = itemsupport.loc[i, 'item_desc']
                packing = itemsupport.loc[i, 'packing']
                upc_11_digit = str(int(itemsupport.loc[i, 'upc_11_digit']))

                duplicate_check = psql.read_sql(f"""
                                                    select * from item_support2
                                                    where code = '{code}' 
                                                    """, connection_pool)
                if len(duplicate_check) == 1:

                    item_support_update(season,
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
                    update += 1

                else:

                    try:

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

                    except Exception as e:
                        print("\nERROR : " + str(e) + f'Quickbook Item:  {code}')

                    insert += 1

                i += 1

        print('\nSupport Sheet Imported')
        print(f'Updated: {update}\nInserted: {insert}')

    def sales_report(self):

        reports = Reports(self.store_type_input, self.store_setting)

        date = datetime.date.today()
        date = date.strftime("%b-%d-%Y")

        filename = f'{self.store_type_input}_external_sales_report_{date}.xlsx'
        reports.external_report(filename)

        filename = f'{self.store_type_input}_internal_sales_report_{date}.xlsx'
        reports.internal_report(filename)

        print("\nSales Report Generated")

    def store_notes_import(self):

        i = 0
        update = 0
        insert = 0

        while i < len(self.store_notes):

            store_id = self.store_notes.iloc[i, 0]
            initial = self.store_notes.iloc[i, 1]
            notes = self.store_notes.iloc[i, 2]
            store_type = self.store_notes.iloc[i, 3]

            duplicate_check = psql.read_sql(f"""select * from {self.store_type_input}.store
                                                            where store_id = '{store_id}' 
                                                         """, self.connection)

            if len(duplicate_check) == 1:

                store_update(store_id, initial, notes, store_type, self.connection_pool, self.store_type_input)

                update += 1

            else:

                store_insert(store_id, initial, notes, store_type, self.connection_pool, self.store_type_input)

                insert += 1

            i += 1

        print('\n Store Data Imported')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def store_pog_import(self):

        i = 0
        update = 0
        insert = 0
        history_inserted = 0

        history = HistoryTracking(self.store_type_input, self.transition_year, self.transition_season, self.connection, self.connection_pool)

        while i < len(self.store_programs):

            store_program_id = self.store_programs.iloc[i, 0]
            store_id = self.store_programs.loc[i, 'STORE']
            program_id = self.store_programs.loc[i, 'PROGRAM']
            activity = self.store_programs.loc[i, 'ACTIVITY']
            store_type = self.store_programs.iloc[i, 4]

            duplicate_check = psql.read_sql(f"""select * from {self.store_type_input}.store_program
                                                where store_program_id = '{store_program_id}' 
                                             """, self.connection)

            if len(duplicate_check) == 1:

                add_history = history.existing_program(store_program_id, store_id, program_id, activity, store_type, duplicate_check)
                store_program_update(store_program_id, store_id, program_id, activity, store_type, self.connection_pool, self.store_type_input)

                update += 1
                history_inserted += add_history

            else:

                add_history = history.new_program(store_program_id,store_id,program_id, activity,store_type)
                store_program_insert(store_program_id, store_id, program_id, activity, store_type, self.connection_pool, self.store_type_input)

                insert += 1
                history_inserted += add_history

            i += 1
        print('\nStore Program Data Imported')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')
        print('Program History Insert:', history_inserted, 'Records')

    def master_planogram_import(self):

        i = 0
        update = 0
        insert = 0

        while i < len(self.master_planogram):

            program_id = self.master_planogram.loc[i, 'Programs']
            cd_ay = self.master_planogram.loc[i, 'CD-AY']
            cd_sn = self.master_planogram.loc[i, 'CD-SN']
            lht_ay = self.master_planogram.loc[i, 'LHT-AY']
            lht_sn = self.master_planogram.loc[i, 'LHT-SN']
            lhd_ay = self.master_planogram.loc[i, 'LHD-AY']
            lhd_sn = self.master_planogram.loc[i, 'LHD-SN']
            lhp_ay = self.master_planogram.loc[i, 'LHP-AY']
            lhp_sn = self.master_planogram.loc[i, 'LHP-SN']
            total_cases = self.master_planogram.loc[i, 'Total Cases']

            duplicate_check = psql.read_sql(f"""select * from master_planogram
                                                where program_id = '{program_id}' 
                                                            """, self.connection)

            if len(duplicate_check) == 1:

                master_planogram_update(program_id, cd_ay, cd_sn, lht_ay, lht_sn, lhd_ay, lhd_sn, lhp_ay, lhp_sn, total_cases, self.connection_pool)

                update += 1

            else:

                master_planogram_insert(program_id, cd_ay, cd_sn, lht_ay, lht_sn, lhd_ay, lhd_sn, lhp_ay, lhp_sn, total_cases, self.connection_pool)
                insert += 1

            i += 1

        print('\nMaster Planogram list imported')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def store_approval_import(self, file):

        transform = TransformData(self.store_type_input, self.transition_year, self.transition_season, self.connection)

        store_approval_df = transform.approval_transform(file)

        store_approval_df_len = len(store_approval_df)

        i = 0
        insert = 0
        update = 0

        while i < store_approval_df_len:

            code = store_approval_df.loc[i, 'code']
            store_price = store_approval_df.loc[i, 'store_price']

            # try and execpt is neccessary right here because POG quickbook codes have a apostophie in them and it
            # messes with the sql below

            try:

                duplicate_check = psql.read_sql(f"""select * from {self.store_type_input}.item_approval
                                                        where code = '{code}' 
               
                                                                    """, self.connection)

                if len(duplicate_check) == 1:

                    item_approval_update(code, store_price, self.connection_pool, self.store_type_input)

                    update += 1

                else:

                    item_approval_insert(code, store_price, self.connection_pool, self.store_type_input)
                    insert += 1

            except:

                pass

            i += 1

        print('\nItem Approval List Imported')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def inventory_import(self, file):

        transform = TransformData(self.store_type_input, self.transition_year, self.transition_season, self.connection)

        inventory_df = transform.inventory_transform(file)

        inventory_df_len = len(inventory_df)

        i = 0
        update = 0
        insert = 0

        while i < inventory_df_len:

            code = inventory_df.iloc[i, 0]
            on_hand = inventory_df.iloc[i, 1]

            duplicate_check = psql.read_sql(f"""select * from inventory
                                                where code = '{code}' 
                                                            """, self.connection)

            if len(duplicate_check) == 1:

                inventory_update(code, on_hand, self.connection_pool)
                update += 1

            else:

                inventory_insert(code, on_hand, self.connection_pool)
                insert += 1

            i += 1

        print('\nInventory list imported')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def size_table_update(self, file):

        size_table = pd.read_excel(f'{file}.xlsx')

        update = 0
        insert = 0
        i = 0

        while i < len(size_table):

            code = size_table.loc[i, 'code']
            size = size_table.loc[i, 'size']

            duplicate_check = psql.read_sql(f"""select * from item_size where code = '{code}' """, self.connection)

            if len(duplicate_check) == 1:

                size_table_update(code, size, self.connection_pool)
                update += 1

            else:

                size_table_insert(code, size, self.connection_pool)
                insert += 1

            i += 1

        print(f"Updated: {update}\nInserted: {insert}\n Store Table Updated")

    def kroger_corporate_report(self):

        reports = Reports(self.store_type_input, self.store_setting)

        reports.kroger_corporate_report()

        print('Kroger Corporate Report Generated')


class DataLocker:

    def __init__(self, store_type_input):

        with EnginePoolDB() as engine:

            self.bandaids_table = psql.read_sql(f"select * from {store_type_input}.bandaids", engine)

            self.delivery_table = psql.read_sql(f"select * from {store_type_input}.delivery2", engine)

            self.item_approval_table = psql.read_sql(f"select * from {store_type_input}.item_approval", engine)

            self.sales_table = psql.read_sql(f"select * from {store_type_input}.sales2", engine)

            self.store_info_table = psql.read_sql(f"select * from {store_type_input}.store", engine)

            self.store_planogram_table = psql.read_sql(f"select * from {store_type_input}.store_program", engine)

            self.store_planogram_history_table = psql.read_sql(f"select * from {store_type_input}.store_program_history", engine)

            self.support_sheet = psql.read_sql(f"select * from public.item_support2", engine)

            self.year_week_verify = psql.read_sql(f"select * from {store_type_input}.year_week_verify", engine)


class StoreRefresh:

    """
    This is the protocol for when any sales member wants to do a Refresh for a given division.

    The purpose of this is when someone want to zero out the inventory for a given store because
    they sent return labels to them and sent those stores  new cases of items.

    The solution to create space for the new items in the planogram is to manually insert band-aids to zero the
    inventory out. However, this creates a problem because if we issue them a band aid right now, then the Credit Memo
    will get registered in the systems 4 months later. Which would in turn create a scenario where there is a
    double entry. To prevent this the following logic will be followed in the code.
    """
    
    def __init__(self, data_locker):
        
        self.data_locker = data_locker
        self.bandaid_table_in_db = self.data_locker.bandaids_table
        self.delivery_table_in_db = self.data_locker.delivery_table

        # Create a df for potential adjustments to the band-aids table
        self.bandaid_col_names = self.bandaid_table_in_db.columns.to_list()
        self.new_bandaid_adjustments = pd.DataFrame(columns=self.bandaid_col_names)

    def bandaid_adjustments_duplication_prevention(self, new_deliv_transform):

        filtered_credit_memo = self.filtered_credit_memo(new_deliv_transform)
        self.delivery_table_duplicate_data_check(filtered_credit_memo)

    def filtered_credit_memo(self, new_deliv_transform):

        """
        Takes the transformed df from the delivery update method in the replenishment class
        and then filters only the credit memos (CM)

        :return filtered df with only (CM) data

        """

        # Band-Aid Duplicate Entry Verification Process
        bandaid_verification = new_deliv_transform[new_deliv_transform['type'] == 'Credit Memo']
        bandaid_verification = bandaid_verification.reset_index(drop=True)

        # inner join the item_support dataframe to get the item_group_desc column
        filtered_credit_memo = bandaid_verification.merge(self.data_locker.support_sheet, on='code', how='left')
        # bandaid_verification = bandaid_verification[['store', 'item_group_desc', 'qty', 'store_type']]

        return filtered_credit_memo

    def delivery_table_duplicate_data_check(self, filtered_credit_memo):
        
        i = 0

        while i < len(filtered_credit_memo):

            invoice_type = filtered_credit_memo.loc[i, 'type_x']

            date_unconverted = filtered_credit_memo.loc[i, 'date']
            date = datetime.strptime(date_unconverted, "%m/%d/%Y").date()

            store_id = int(filtered_credit_memo.loc[i, 'store'])
            upc = filtered_credit_memo.loc[i, 'upc_x']
            store_type = filtered_credit_memo.loc[i, 'store_type']
            code = filtered_credit_memo.loc[i, 'code']
            num = filtered_credit_memo.loc[i, 'num']
            item_group_desc = filtered_credit_memo.loc[i, 'item_group_desc']

            # check if cm qty is negative
            credit_memo_qty = filtered_credit_memo.loc[i, 'qty']

            if credit_memo_qty <= 0:
                pass
            else:
                raise Exception(f"Credit Memo Qty is not negative value: {credit_memo_qty}")


            duplicate_check = self.delivery_table_in_db[(self.delivery_table_in_db['date'] == date) &
                                                   (self.delivery_table_in_db['type'] == invoice_type) &
                                                   (self.delivery_table_in_db['store'] == store_id) &
                                                   (self.delivery_table_in_db['upc'] == upc) &
                                                   (self.delivery_table_in_db['store_type'] == store_type) &
                                                   (self.delivery_table_in_db['code'] == code) &
                                                   (self.delivery_table_in_db['num'] == num)
                                                   ]

            if len(duplicate_check) == 1:
                # if data is already in the delivery table then no need to adjust bandaid table
                pass

            elif len(duplicate_check) == 0:

                self.bandaid_duplication_check(store_id, item_group_desc, credit_memo_qty, date, store_type)

            else:
                raise Exception(f"Error: Possible Duplicate Entry. Investigate error invoice num: {num}")

            i += 1

    def bandaid_duplication_check(self, store_id, item_group_desc, credit_memo_qty, date, store_type):

        # sum all bandaids in the db for store-item. If it's >=0 then ignore if < 0 then adjust

        # filter then add everything up
        bandaid_table_in_db_filtered = self.bandaid_table_in_db[(self.bandaid_table_in_db['store_id'] == store_id) &
                                                                (self.bandaid_table_in_db[
                                                                     'item_group_desc'] == item_group_desc)
                                                                ]

        if len(bandaid_table_in_db_filtered) >= 1:

            sum_db_bandaid_qty = bandaid_table_in_db_filtered['qty'].sum()
            self.create_bandaid_adjustments(sum_db_bandaid_qty, credit_memo_qty, store_id, item_group_desc, date, store_type)

        elif len(bandaid_table_in_db_filtered) == 0:
            pass
        else:
            raise Exception('Error: Should not happen')

    def create_bandaid_adjustments(self, sum_db_bandaid_qty, credit_memo_qty, store_id, item_group_desc, date, store_type):

        date_created = datetime.now().date()

        # less (cm -13  bandaid -50) or exact (cm -50  bandaid -50)
        if credit_memo_qty >= sum_db_bandaid_qty:

            bandaid_adjustment_qty = credit_memo_qty * -1

            reason = 'Refresh Program:CM entered in DB. Adjusted bandaid to prevent duplicate entry'

        # more cm -60  bandaid -50
        elif credit_memo_qty < sum_db_bandaid_qty:

            bandaid_adjustment_qty = sum_db_bandaid_qty * -1

            reason = 'Refresh Program:CM entered in DB. Adjusted bandaid and brought the sum of Bandaid back to zero'

        else:
            raise Exception("Error: Should not happen")

        dictionary = {'type': 'Bandaid',
                      'store_id': store_id,
                      'item_group_desc': item_group_desc,
                      'qty': bandaid_adjustment_qty,
                      'date_created': date_created,
                      'effective_date': date,
                      'store_type': store_type,
                      'reason': f'{reason}'
                      }

        potential_bandaids_adjustments = pd.DataFrame(dictionary, index=[0])

        self.new_bandaid_adjustments = pd.concat([self.new_bandaid_adjustments, potential_bandaids_adjustments],
                                                 ignore_index=True)

        self.bandaid_table_in_db = pd.concat([self.bandaid_table_in_db, potential_bandaids_adjustments],
                                             ignore_index=True)
