import psycopg2
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import pool
import numpy as np
import datetime
from IPython.display import display


from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def alignment_insert(store_number,
                     division,
                     region,
                     district,
                     division_vp,
                     district_leader,
                     region_director,
                     store_manager,
                     division_vp_email,
                     region_director_email,
                     district_leader_email,
                     dpc,
                     dpc_email,
                     connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        """INSERT INTO alignment (store_number,
                                  division,
                                  region,
                                  district,
                                  division_vp,
                                  district_leader,
                                  region_director,
                                  store_manager,
                                  division_vp_email,
                                  region_director_email,
                                  district_leader_email,
                                  dpc,
                                  dpc_email) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (store_number,
         division,
         region,
         district,
         division_vp,
         district_leader,
         region_director,
         store_manager,
         division_vp_email,
         region_director_email,
         district_leader_email,
         dpc,
         dpc_email))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)

def alignment_update(store_number,
                     division,
                     region,
                     district,
                     division_vp,
                     district_leader,
                     region_director,
                     store_manager,
                     division_vp_email,
                     region_director_email,
                     district_leader_email,
                     dpc,
                     dpc_email,
                     connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"""update alignment set division = {division},
                     region = {region},
                     district= {district},
                     division_vp = '{division_vp}',
                     district_leader = '{district_leader}',
                     region_director = '{region_director}',
                     store_manager = '{store_manager}',
                     division_vp_email = '{division_vp_email}',
                     region_director_email = '{region_director_email}',
                     district_leader_email= '{district_leader_email}',
                     dpc = '{dpc}',
                     dpc_email = '{dpc_email}'
                     where store_number = {store_number}
                    """)
    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)

def fm_store_insert(store_number, fm, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        """INSERT INTO fm_store (store_number, fm) 
            values (%s,%s)""",
        (store_number,fm))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def fm_store_update(store_number, fm, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""update fm_store set fm = {fm} 
            where store_number = {store_number}""",
        (store_number, fm))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def store_sales_import(fmm, division, region, district, store_number, state, total_prev_year, prev_year, current_year, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        """INSERT INTO store_sales (fmm, division, region, district, store_number, state, total_prev_year, prev_year,
                                    current_year) 
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (fmm, division, region, district, store_number, state, total_prev_year, prev_year, current_year))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)



class CVS():

    def __init__(self):

        self.connection_pool = pool.SimpleConnectionPool(1, 10000,
                                                         database="CVS",
                                                         user="postgres",
                                                         password="winwin",
                                                         host="localhost")

        self.connection = psycopg2.connect(database=f"CVS", user="postgres", password="winwin", host="localhost")


    def alignment(self, file):

        """Imports alignment sheet"""

        alignment = pd.read_excel(f'{file}')

        update = 0
        insert = 0
        i = 0

        while i < len(alignment):

            store_number = alignment.loc[i,'Store #']
            division = alignment.loc[i,"Division"]
            region = alignment.loc[i ,"Region"]
            district = alignment.loc[i,"District"]
            division_vp = alignment.loc[i,"Division VP"]
            district_leader = alignment.loc[i,"District Leader"]
            region_director = alignment.loc[i,"Region Director"]
            store_manager = alignment.loc[i,"Store Mgr"]
            division_vp_email = alignment.loc[i,"Division VP\xa0Email"]
            region_director_email = alignment.loc[i,"Region\xa0Dir Email"]
            district_leader_email = alignment.loc[i,"District\xa0Leader Email"]
            dpc = alignment.loc[i,"DPC"]
            dpc_email = alignment.loc[i,"DPC Email"]

            duplicate_check = psql.read_sql(f"""
                                                select * from alignment
                                                where store_number = '{store_number}' 
                                                """, self.connection)

            if len(duplicate_check) == 1:

                alignment_update(store_number,
                                 division,
                                 region,
                                 district,
                                 division_vp,
                                 district_leader,
                                 region_director,
                                 store_manager,
                                 division_vp_email,
                                 region_director_email,
                                 district_leader_email,
                                 dpc,
                                 dpc_email,
                                 self.connection_pool)
                update += 1

            else:

                try:

                    alignment_insert(store_number,
                                     division,
                                     region,
                                     district,
                                     division_vp,
                                     district_leader,
                                     region_director,
                                     store_manager,
                                     division_vp_email,
                                     region_director_email,
                                     district_leader_email,
                                     dpc,
                                     dpc_email,
                                     self.connection_pool)

                except Exception as e:
                    print("\nERROR : " + str(e) + f'store #:  {store_number}' )

                insert += 1

            i += 1

        print('\nAlignment Imported')
        print(f'Updated: {update}\nInserted: {insert}')

    def fm_store_import(self, file):

        """Imports using CVS Program Case Space Capacity Sheet found in data team folder """


        fm_store = pd.read_excel(f'{file}')

        update = 0
        insert = 0
        i = 0

        while i < len(fm_store):

            store_number = fm_store.loc[i, 'Store']
            fm = fm_store.loc[i, "FM"]


            duplicate_check = psql.read_sql(f"""
                                                select * from fm_store
                                                where store_number = '{store_number}' 
                                                """, self.connection)

            if len(duplicate_check) == 1:

                fm_store_update(store_number,
                                 fm,

                                 self.connection_pool)
                update += 1

            else:

                try:

                    fm_store_insert(store_number,
                                     fm,
                                     self.connection_pool)

                except Exception as e:
                    print("\nERROR : " + str(e) + f'store #:  {store_number}')

                insert += 1

            i += 1

        print('\nFM Store Imported')
        print(f'Updated: {update}\nInserted: {insert}')

    def store_sales(self, file):

        """This data is copied from tab 'D1' in the comprehensive dashboard and then pasted to another excel file"""

        store_sales = pd.read_excel(f'{file}')

        insert = 0
        i = 0

        while i < len(store_sales):

            fmm = store_sales.loc[i, "FMM"]
            division = store_sales.loc[i,"Div"]
            region = store_sales.loc[i,'Reg']
            district = store_sales.loc[i,'Dis']
            store_number = store_sales.loc[i, 'Store']
            state = store_sales.loc[i,'State']
            total_prev_year = store_sales.loc[i,'2021 $ Total']
            prev_year = store_sales.loc[i,2021]
            current_year = store_sales.loc[i,2022]


            try:

                store_sales_import(fmm, division, region, district, store_number, state,
                                   total_prev_year, prev_year, current_year, self.connection_pool)
                insert += 1


            except Exception as e:
                print("\nERROR : " + str(e) + f'store #:  {store_number}')

            i += 1

        print('\nStore Sales Imported')
        print(f'Inserted: {insert}')

    def callout_ghost_letter(self):

        """Method used to print MonthLy Callout Letter for FMM uses the CVS database need to delete and inset data into
        alignment and store_sales table monthly"""

        print("Which FMM Ghost Letter Do you Want to Create (select a number)\n")
        print("1. Buneta\n2. Post\n3. Kolenda\n4. Anderson\n")

        i = True

        while i == True:

            fmm = int(input("FMM: \t"))
            print('\n')

            if fmm == 1:

                names = "Jon and Ted"
                fmm = "Buneta"
                i = False

            elif fmm == 2:

                names = "Steve"
                fmm = "Post"
                i = False

            elif fmm == 3:

                names = "Lisa and Marji"
                fmm = "Kolenda"
                i = False

            elif fmm == 4:

                names = 'Paul and Colleen'
                fmm = "Anderson"
                i = False

            else:

                print("Please select a number from 1-4")

        print('What month is this ghost letter for?')
        print("""

        1.\tJanuary
        2.\tFebruary
        3.\tMarch
        4.\tApril
        5.\tMay
        6.\tJune
        7.\tJuly
        8.\tAugust
        9.\tSeptember
        10.\tOctober
        11.\tNovember
        12.\tDecember

        """)

        i = True

        while i == True:

            month_number = int(input("Month: \t"))
            print('\n')

            if month_number == 1:

                month = "January"
                i = False

            elif month_number == 2:

                month = "February"
                i = False

            elif month_number == 3:

                month = "March"
                i = False

            elif month_number == 4:

                month = "April"
                i = False

            elif month_number == 5:

                month = "May"
                i = False

            elif month_number == 6:

                month = "June"
                i = False

            elif month_number == 7:

                month = "July"
                i = False

            elif month_number == 8:

                month = "August"
                i = False

            elif month_number == 9:

                month = "September"
                i = False

            elif month_number == 10:

                month = "October"
                i = False

            elif month_number == 11:

                month = "November"
                i = False

            elif month_number == 12:

                month = "December"
                i = False

            else:

                print("Please select a number from 1-12")

        # defining variable for date for fstring later
        date = datetime.datetime.now()
        year = date.year

        # this will do a query to produce a table that shows how many millions of dollars the fmm has made and the percentage
        # difference compared between this year and last year. Then I will take that data to assign to variable for the fstring later

        sales_total = f"""

        with tab as (

            select sum(current_year) as current_year, sum(prev_year) as previous
            from store_sales where fmm = '{fmm}')

        select round((current_year/1000000),1) as this_year, round(((current_year-previous)/previous)*100) as percent_diff  
        from tab



        """

        sales_total = psql.read_sql(f'{sales_total}', self.connection)

        total_sales = sales_total.loc[0, 'this_year']
        percent_diff = sales_total.loc[0, 'percent_diff'].astype(int)

        # finds the top 3 Regions and theirs sales $ in thousands

        region = f"""


        select region, round(sum(current_year)/1000,1) as sales
        from store_sales where fmm = '{fmm}'
        group by region
        order by sum(current_year) desc



        """

        region = psql.read_sql(f'{region}', self.connection)

        region1 = region.loc[0, 'region']
        region1_sales = region.loc[0, 'sales']

        region2 = region.loc[1, 'region']
        region2_sales = region.loc[1, 'sales']

        region3 = region.loc[2, 'region']
        region3_sales = region.loc[2, 'sales']

        # looks for top 3 region directors names

        def region_director_name_lookup(region):

            region_director_name_lookup = f"""


            select * from alignment
            where region = {region}



            """

            region = psql.read_sql(f'{region_director_name_lookup}', self.connection)

            region = region.loc[0, 'region_director']

            # this is neccesary because the cvs raw file has the columns formated with "\xa0" in between each string
            name = region.replace("\xa0", " ")

            return name

        reg1_director_name = region_director_name_lookup(region1)

        reg2_director_name = region_director_name_lookup(region2)

        reg3_director_name = region_director_name_lookup(region3)

        display(region)
        print('Enter the sales $ amount you want to shout out for Regions')
        region_dollar = int(input('Sales $:\t'))

        region_top_performer = region[region['sales'] >= region_dollar]

        # finds the top 3 Divisions and theirs sales $ in thousands

        district = f"""


        select region, district, round(sum(current_year)/1000,1) as sales
        from store_sales where fmm = '{fmm}'
        group by region, district
        order by sum(current_year) desc




        """

        district = psql.read_sql(f'{district}', self.connection)

        rd1 = district.loc[0, 'region']
        rd1_sales = district.loc[0, 'sales']
        district1 = district.loc[0, 'district']

        rd2 = district.loc[1, 'region']
        rd2_sales = district.loc[1, 'sales']
        district2 = district.loc[1, 'district']

        rd3 = district.loc[2, 'region']
        rd3_sales = district.loc[2, 'sales']
        district3 = district.loc[2, 'district']

        # looks for top 3 district directors names

        def district_director_name_lookup(region, district):

            district_director_name_lookup = f"""


            select * from alignment
            where region = {region} and
            district = {district}



            """

            region = psql.read_sql(f'{district_director_name_lookup}', self.connection)

            region = region.loc[0, 'district_leader']

            # this is neccesary because the cvs raw file has the columns formated with "\xa0" in between each string
            name = region.replace("\xa0", " ")

            return name

        dis1_director_name = district_director_name_lookup(rd1, district1)
        dis2_director_name = district_director_name_lookup(rd2, district2)
        dis3_director_name = district_director_name_lookup(rd3, district3)

        display(district.head(20))
        print('Enter the sales $ amount you want to shout out for Districts')
        district_dollar = int(input('Sales $:\t'))

        district_top_performer = district[district['sales'] >= district_dollar]

        # finds the top 3 Stores and theirs sales $ in thousands

        stores = f"""


        select store_number, round(sum(current_year)/1000,1) as sales
        from store_sales where fmm = '{fmm}'
        group by store_number
        order by sum(current_year) desc


        """

        store = psql.read_sql(f'{stores}', self.connection)

        store1 = store.loc[0, 'store_number']
        store1_sales = store.loc[0, 'sales']

        store2 = store.loc[1, 'store_number']
        store2_sales = store.loc[1, 'sales']

        store3 = store.loc[2, 'store_number']
        store3_sales = store.loc[2, 'sales']

        display(store.head(20))
        print('Enter the sales $ amount you want to shout out for Stores')
        store_dollar = int(input('Sales $:\t'))

        store_top_performer = store[store['sales'] >= store_dollar]

        print(f"""\n

{fmm} Team - WinWin {month} {year} Actionable Comprehensive Dashboard


Hi {names},\n

Your monthly WinWin {month} {year} actionable comprehensive dashboard is attached with callouts to share with your team below.\n
Excludes face mask sales. {month} YTD sales are up {percent_diff}% at {total_sales}M








Team,


Attached is your WinWin {month} {year} actionable comprehensive dashboard to help identify gaps and opportunities. Includes Region, District,
and Store level reporting tabs. Here’s a recap of some key sales callouts below.


    •   {len(region_top_performer)} Regions are above ${region_dollar}K YTD to include{reg1_director_name} (R{region1} @ ${region1_sales}K),{reg2_director_name} (R{region2} @ ${region2_sales}K),{reg3_director_name} (R{region3} @ ${region3_sales}K)

    •   {len(district_top_performer)} Districts are above ${district_dollar}K YTD to include{dis1_director_name} (R{rd1}-D{district1} @ {rd1_sales}K),{dis2_director_name} (R{rd2} -D{district2} @ {rd2_sales}K),{dis3_director_name} (R{rd3}-D{district3} @ {rd3_sales}K)

    •   {len(store_top_performer)} Stores are above ${store_dollar}K  YTD to include store {store1} (${store1_sales}K), store {store2} (${store2_sales}K), store {store3} (${store3_sales}K)



        """)





