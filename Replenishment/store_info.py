

from Import.data_insertion import *
from Update.Transform_Sales_Data.transform import *
from Sales_Report.Reports.reports import *
from Sales_Report.Replenishment.replenishment import *



class Replenishment():

    def __init__(self, store_type_input):
        
        self.store_type_input = store_type_input
        
        self.connection_pool = pool.SimpleConnectionPool(1, 10000, 
                                            database= "test",
                                            user="postgres", 
                                            password="winwin", 
                                            host="localhost")

        self.connection = psycopg2.connect(database=f"test", user="postgres", password="winwin", host="localhost")

        # self.store_setting = pd.read_excel(rf'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
        #                                    sheet_name='Sheet2',
        #                                    header=None)
        self.store_setting= pd.read_excel(rf'C:\Users\User1\OneDrive - winwinproducts.com\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                            sheet_name='Sheet2',
                                            header=None,
                                            index_col=0,
                                            names=('setting', 'values'))

        self.transition_year = self.store_setting.loc['Transition_year', 'values']
        self.transition_season = self.store_setting.loc['Transition_Season','values']

        self.store_programs = pd.read_excel(rf'C:\Users\User1\OneDrive - winwinproducts.com\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                            sheet_name='Store Programs')

        self.store_notes = pd.read_excel(rf'C:\Users\User1\OneDrive - winwinproducts.com\Groccery Store Program\{self.store_type_input}\{self.store_type_input}_store_setting.xlsm',
                                         sheet_name='Store Notes')

        self.master_planogram = pd.read_excel(rf'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Replenishment program\Replenishment\support document\MASTER PLANOGRAM.xlsx',
                                         sheet_name='MASTER PLANOGRAM')

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
            code = new_deliv.iloc[i,9]

            if store_type != self.store_type_input:
                print(f'Data Validation Failed Inserted {store_type} for {self.store_type_input} database')
                break

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

        if store_type == self.store_type_input:

            print(f'\n {self.store_type_input} Delivery Data Imported')

    def delivery_update(self, file):

        '''Takes in csv file of delivery data from qb and converts it to proper formating so it can be
        imported into postgres'''

        connection = self.connection_pool.getconn()

        df = pd.read_csv(f'{file}')

        # drops uneccesary columns
        df = df.drop(columns=['Unnamed: 0', 'U/M', 'Sales Price', 'Amount', 'Balance'])
        df = df.dropna()

        # filters out unnecesary qb types only invoice or credit memo
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

        elif self.store_type_input == 'follett':
            df[['store_type', 'letter1']] = df.Name.str.split(':', n=1, expand=True)
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
            'FRESH ENCOUNTER': 'fresh_encounter',
            'KROGER KING SOOPERS': 'kroger_king_soopers',
            'KROGER DILLONS': 'kroger_dillons',
            'KROGER CINCINNATI': 'kroger_cincinatti',
            'KROGER ATLANTA': 'kroger_atlanta',
            'KROGER NASHVILLE': 'kroger_nashville',
            'KROGER LOUISVILLE': 'kroger_louisville',
            'FOLLETT': 'follett'

        }

        store_type = df.iloc[0, 9]

        df['store_type'] = store_list[store_type]

        # renaming collumns and putting them in the right order

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

        new_deliv_transform = new_deliv_transform.reset_index(drop=True)

        ######LOADING DATA INTO POSTGRES WITH PYTHON #########

        new_deliv_transform_length = len(new_deliv_transform)
        i = 0
        update = 0
        insert = 0

        while i < new_deliv_transform_length:

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
                                upc, store, qty, store_type, num, code, self.connection_pool,self.store_type_input)
                update += 1
            else:
                delivery_insert(transition_year,
                                transition_season,
                                type, date, upc, store, qty, store_type,
                                num, code, self.connection_pool, self.store_type_input)
                insert += 1

            i += 1

        self.connection.commit()

        print(f'\n {self.store_type_input} Delivery Data Updated')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def sales_import(self, file):

        """'This takes in an excel file and inserts sales data into postgres
         Data collumns must be in this order

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
                print(f'Data Validation Failed Inserted data for {store_type} for {self.store_type_input} database')
                break

            sales_insert(transition_year, transition_season, store_year, date,
                         store_week, store_number, upc, sales,
                         qty, current_year, current_week, code, store_type,
                         self.connection_pool, self.store_type_input)
            i += 1

        if store_type == self.store_type_input:
            print(f'\n {self.store_type_input} Sales Data Imported')

    def sales_update(self, file):

        connection = self.connection_pool.getconn()

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

        transform = TransformData(self.store_type_input, self.transition_year, self.transition_season, connection)

        # series of if statement used to determine which program to use to transform the data to the proper format.

        if self.store_type_input in kroger:

            sales_data = transform.kroger_transform(file)

        elif self.store_type_input == 'kvat':

            sales_data = transform.kvat_transform(file)

        elif self.store_type_input == 'safeway_denver':

            sales_data = transform.safeway_denver_transform(file)

        elif self.store_type_input == 'jewel':

            sales_data = transform.jewel_transform(file)

        else:
            raise Exception('Error: Sales update method is not established for this store')

        i = 0
        update = 0
        insert = 0
        inserted_list = []

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

            #verifying data to make sure correct data is being inserted into the correct database
            if store_type == self.store_type_input:
                pass
            else:
                raise Exception(f'Error: {store_type} is trying to be inserted into the {self.store_type_input} table')

            duplicate_check = psql.read_sql(f"""
                                                SELECT * FROM {self.store_type_input}.SALES2 
                                                WHERE store_year ={store_year} and 
                                                    store_week = '{store_week}' and
                                                    date = '{date}' and 
                                                    store_number = {store_number} and 
                                                    upc = '{upc}' and 
                                                    store_type = '{store_type}'
                                            """, connection)

            duplicate_check_len = len(duplicate_check)

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
                            self.connection_pool,
                            self.store_type_input)
                update += 1
            else:
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
                             self.connection_pool,
                             self.store_type_input)
                insert += 1
                inserted_list.append(i)

            i += 1

            connection.commit()

        self.connection_pool.putconn(connection)

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

        itemsupport = pd.read_excel(f'{file}.xlsx')

        update = 0
        insert = 0

        item_support = len(itemsupport)
        i = 0

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
                                                """, self.connection)

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
                    print("\nERROR : " + str(e) + f'Quickbook Item:  {code}' )

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

    def store_import(self):

        new_len = len(self.store_notes)
        i = 0
        update = 0
        insert = 0

        while i < new_len:

            store_id = self.store_notes.iloc[i, 0]
            initial = self.store_notes.iloc[i, 1]
            notes = self.store_notes.iloc[i, 2]

            duplicate_check = psql.read_sql(f"""select * from {self.store_type_input}.store
                                                            where store_id = '{store_id}' 
                                                         """, self.connection)

            if len(duplicate_check) == 1:

                store_update(store_id, initial, notes, self.connection_pool, self.store_type_input)

                update += 1

            else:

                store_insert(store_id, initial, notes, self.connection_pool, self.store_type_input)

                insert += 1

            i += 1

        print('\n Store Data Imported')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def store_program_import(self):

        new_len = len(self.store_programs)
        i = 0
        update = 0
        insert = 0
        while i < new_len:

            store_program_id = self.store_programs.iloc[i, 0]
            store_id = self.store_programs.iloc[i, 1]
            program_id = self.store_programs.iloc[i, 2]
            activity = self.store_programs.iloc[i, 3]

            duplicate_check = psql.read_sql(f"""select * from {self.store_type_input}.store_program
                                                where store_program_id = '{store_program_id}' 
                                             """, self.connection)

            if len(duplicate_check) == 1:

                store_program_update(store_program_id,store_id, program_id, activity,self.connection_pool, self.store_type_input)
                update += 1

            else:

                store_program_insert(store_program_id, store_id, program_id, activity, self.connection_pool, self.store_type_input)
                insert += 1

            i += 1
        print('\n Store Program Data Imported')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def master_planogram_import(self):

        i = 0
        update = 0
        insert = 0

        while i < len(self.master_planogram):

            program_id = self.master_planogram.iloc[i, 0]
            carded = self.master_planogram.iloc[i, 1]
            long_hanging_top = self.master_planogram.iloc[i, 2]
            long_hanging_dress = self.master_planogram.iloc[i, 3]


            duplicate_check = psql.read_sql(f"""select * from master_planogram
                                                where program_id = '{program_id}' 
                                                            """, self.connection)

            if len(duplicate_check) == 1:

                master_planogram_update(program_id, carded, long_hanging_top, long_hanging_dress, self.connection_pool)

                update+=1

            else:

                master_planogram_insert(program_id, carded, long_hanging_top, long_hanging_dress, self.connection_pool)
                insert+=1

            i += 1

        print('\nMaster Planogram list imported')
        print('Updated:', update, 'Records')
        print('Inserted:', insert, 'Records')

    def store_approval_import(self, file):

        transform = TransformData(self.store_type_input, self.transition_year, self.transition_season, connection)

        store_approval_df = transform.approval_transform(self.store_type_input, file)

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

        transform = TransformData(self.store_type_input, self.transition_year, self.transition_season, connection)

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
                update+=1

            else:

                inventory_insert(code, on_hand, self.connection_pool)
                insert+=1

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

                size_table_update(code,size,self.connection_pool)
                update+=1

            else:

                size_table_insert(code,size,self.connection_pool)
                insert+=1

            i+=1

        print(f"Updated: {update}\nInserted: {insert}\n Store Table Updated")





