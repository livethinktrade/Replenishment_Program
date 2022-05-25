import psycopg2
import pandas as pd
import pandas.io.sql as psql
from datetime import timedelta

import numpy as np


class Reports:

    def __init__(self, store_type_input, store_setting):

        self.store_type_input = store_type_input

        self.store_setting = store_setting

        self.connection = psycopg2.connect(database=f"{self.store_type_input}",
                                           user="postgres",
                                           password="winwin",
                                           host="localhost")

        if store_type_input in ['kroger_dallas',
                                'kroger_central',
                                'kroger_delta',
                                'kroger_michigan',
                                'kroger_columbus',
                                'kroger_atlanta',
                                'kroger_cincinnati',
                                'kroger_dillons',
                                'kroger_king_sooper',
                                'kroger_louisville',
                                'kroger_nashville']:

            self.week = 'store_week'
        else:
            self.week = 'current_week'


        # finds the current year per winwin company year
        year = psql.read_sql("select max(current_year) from sales;", self.connection)

        # finds the current week the store is on
        self.store_year = year.iloc[0, 0]

        num = psql.read_sql(f"select max({self.week}) from sales where store_year = {self.store_year}", self.connection)
        self.week_num = num.iloc[0, 0]

        # This line is neccesary for jewel because when the sales data comes in on monday it usually
        # includes sales that were made the previous day on Sunday. This is a problem because
        # Sunday marks the beginning of the new week
        # which meesses up the weekly reporting

        if store_type_input == 'jewel':
            self.week_num -= 1

        # finds the number of weeks the store has been active in the current year.
        # more important for new stores rather than old.
        # helps calculate the avg store sales

        num = psql.read_sql(f"select distinct({self.week}) from sales where store_year = {self.store_year}", self.connection)
        self.num = len(num)

    def sales_table(self):

        # creates sales table along with sales $ per store and previous year performance

        sales_sql = f"""
            with date as ( 
                select distinct store_number 
                from sales
                order by store_number asc),

                current_week as (

                    select store_number, store_year, {self.week}, sum(sales) as sales 
                    from sales
                    inner join item_support on sales.upc = item_support.upc_11_digit

                    where store_year = {self.store_year} and
                        {self.week} = {self.week_num} and 
                        (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW') 

                    group by store_number, store_year, {self.week}

                    order by store_number asc),

                previous_week as (

                    select store_number, store_year, {self.week}, sum(sales) as sales 
                    from sales
                    inner join item_support on sales.upc = item_support.upc_11_digit

                    where store_year = {self.store_year} and
                        {self.week} = {self.week_num}-1 and 
                        (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW') 

                    group by store_number, store_year, {self.week}

                    order by store_number asc),

                previous_year_week as (
                    select store_number, store_year, {self.week}, sum(sales) as sales 
                    from sales
                    inner join item_support on sales.upc = item_support.upc_11_digit

                    where store_year = {self.store_year}-1 and
                        {self.week} = {self.week_num} and 
                        (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW') 

                    group by store_number, store_year, {self.week}

                    order by store_number asc),

                current_ytd_week as (

                    select store_number, store_year, sum(sales) as sales 
                    from sales
                    inner join item_support on sales.upc = item_support.upc_11_digit

                    where store_year = {self.store_year} and
                        {self.week} <= {self.week_num} and 
                        (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW') 

                    group by store_number, store_year

                    order by store_number asc),


                previous_ytd_week as (


                    select store_number, store_year, sum(sales) as sales 
                    from sales
                    inner join item_support on sales.upc = item_support.upc_11_digit
                    /*looks for previous year so maybe max -1 when finding innitial start for python program*/
                    where store_year = {self.store_year}-1 and
                        {self.week} <= {self.week_num} and 
                        (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW') 

                    group by store_number, store_year

                    order by store_number asc)

            select
                date.store_number,
                current_week.sales as current_week,
                previous_week.sales as previous_week,
                /*WOW sales % */
                case
                    when current_week.sales < 0 or previous_week.sales <= 0
                        then NULL
                    when current_week.sales >= 0 or previous_week.sales > 0
                        then round(((current_week.sales-previous_week.sales)/(previous_week.sales)),2)
                    end as WOW_sales_percentage,



                current_week.sales as current_week,
                previous_year_week.sales as previous_year_week,
                case
                    when current_week.sales < 0 or previous_year_week.sales <= 0
                        then NULL
                    when current_week.sales >= 0 or previous_year_week.sales > 0
                        then round(((current_week.sales-previous_year_week.sales)/(previous_year_week.sales)),2)
                    end as YoY_sales_percentage,


                current_ytd_week.sales as YTD_2022,
                previous_ytd_week.sales as YTD_2021,
                case
                    when current_ytd_week.sales < 0 or previous_ytd_week.sales <= 0
                        then NULL
                    when current_ytd_week.sales >= 0 or previous_ytd_week.sales > 0
                        then round(((current_ytd_week.sales-previous_ytd_week.sales)/(previous_ytd_week.sales)),2)
                    end as YoY_sales_percentage


            from date
            full join current_week on date.store_number = current_week.store_number
            full join previous_week on date.store_number = previous_week.store_number
            full join previous_year_week on date. store_number = previous_year_week.store_number
            full join current_ytd_week on date.store_number = current_ytd_week.store_number
            full join previous_ytd_week on date.store_number = previous_ytd_week.store_number 

            order by current_ytd_week.sales desc

            """

        sales_report = psql.read_sql(f'{sales_sql}', self.connection)

        return sales_report

    def sales_report_len(self, sales_report):

        sales_report_len = len(sales_report) + 1

        return sales_report_len

    def ytd_table_no_mask(self):

        # This will generate YTD Sales and preveious YTD sales "Without Mask"

        sales_sql_YTD_WoMask = f"""

        with 
            store_count as (
                select distinct store_number 
                from sales
                where store_year = {self.store_year}),


            current_ytd_week as (

                select store_number, store_year, sum(sales) as sales 
                from sales
                inner join item_support on sales.upc = item_support.upc_11_digit

                where store_year = {self.store_year} and
                    {self.week} <= {self.week_num} and 
                    (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW') 

                group by store_number, store_year

                order by store_number asc),

            previous_ytd_week as (


                select store_number, store_year, sum(sales) as sales 
                from sales
                inner join item_support on sales.upc = item_support.upc_11_digit

                /*looks for previous year so maybe max -1 when finding innitial start for python program*/
                where store_year = {self.store_year}-1 and
                    {self.week} <= {self.week_num} and 
                    (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW') 

                group by store_number, store_year

                order by store_number asc),

            yearly_sum as (

                select round(sum(sales)) as YTD_current, 
                    (select round(sum(sales)) as YTD_previous from previous_ytd_week)
                from current_ytd_week),

            yearly_sum_p2 as (
                select ytd_current, ytd_previous, 
                        ((ytd_current-ytd_previous)/ytd_previous) as YoY_Change,
                    (select count(store_number) from store_count) as store_supported
                from yearly_sum)

        select 
            ytd_current, 
            ytd_previous, 
            yoy_change, 

            store_supported,
            /*change number to current week of store year to make program dynamic
            next line calculates Avg Sales Per Wk/Store ($)*/
            round(ytd_current/store_supported/{self.num}) as avg_wk_store, 

            /*next line calculates Avg Sales Per month/Store ($)*/
            round((ytd_current/store_supported/{self.num})*4) as avg_month_store

        from yearly_sum_p2
        """

        sales_ytd_no_mask = psql.read_sql(f'{sales_sql_YTD_WoMask}', self.connection)

        return sales_ytd_no_mask

    def ytd_table_mask(self):

        sales_sql_YTD_Mask = f"""

            with 
            
                /*finds all of the store numbers in the database*/
                store_count as (
                    select distinct store_number 
                    from sales
                    where store_year = {self.store_year}),


                current_ytd_week as (

                    select store_number, store_year, sum(sales) as sales 
                    from sales
                    inner join item_support on sales.upc = item_support.upc_11_digit

                    where store_year = {self.store_year} and
                        {self.week} <= {self.week_num}

                    group by store_number, store_year

                    order by store_number asc),

                previous_ytd_week as (


                    select store_number, store_year, sum(sales) as sales 
                    from sales
                    inner join item_support on sales.upc = item_support.upc_11_digit

                    /*looks for previous year so maybe max -1 when finding innitial start for python program*/
                    where store_year = {self.store_year}-1 and
                        {self.week} <= {self.week_num}

                    group by store_number, store_year

                    order by store_number asc),

                yearly_sum as (

                    select round(sum(sales)) as YTD_current, 
                        (select round(sum(sales)) as YTD_previous from previous_ytd_week)
                    from current_ytd_week),

                yearly_sum_p2 as (
                    select ytd_current, ytd_previous, 
                            ((ytd_current-ytd_previous)/ytd_previous) as YoY_Change,
                        (select count(store_number) from store_count) as store_supported
                    from yearly_sum)

            select 
                ytd_current, 
                ytd_previous, 
                yoy_change, 

                store_supported,
                /*change number to current week of store year to make program dynamic
                next line calculates Avg Sales Per Wk/Store ($)*/
                round(ytd_current/store_supported/{self.num}) as avg_wk_store, 

                /*next line calculates Avg Sales Per month/Store ($)*/
                round((ytd_current/store_supported/{self.num})*4) as avg_month_store

            from yearly_sum_p2
            """

        sales_sql_YTD_Mask = psql.read_sql(f'{sales_sql_YTD_Mask}', self.connection)

        return sales_sql_YTD_Mask

    def item_sales_rank(self):

        #produce list of items ranked based off of sales for current year.
        #gives the item, sales, units sold, percentage of sales per item, and season

        item_sales_rank = f'''
        with 
            year_total as(
                Select sum(sales) as year_sales, sum(qty) as year_total_unit
                from sales
                where store_year = {self.store_year}
            ),

            item_sales_rank as (
                Select item_group_desc, round(sum(sales),2) as sales, sum(qty) as units_sold,season
                from sales
                inner join item_support on sales.upc = item_support.upc_11_digit
                where store_year = {self.store_year}
                group by item_group_desc, season
                order by sum(sales) desc)


        select item_group_desc, 
            sales, 
            units_sold, 
            round((sales/year_sales),3) as percent_of_total_sales,
            season
        from item_sales_rank, year_total
        '''

        item_sales_rank = psql.read_sql(f'{item_sales_rank}', self.connection)
        item_sales_rank = item_sales_rank.head(10)

        return item_sales_rank

    def on_hands(self):

        # on_hand report
        on_hand = psql.read_sql("SELECT * FROM SD_COMBO WHERE season = 'AY' or season = 'SS' order by store asc",
                                self.connection)

        return on_hand

    def no_scan(self):

        # Assigning variable from store seeting sheet 2 table.

        initial_display = self.store_setting.iloc[0, 1]

        in_season = self.store_setting.iloc[1, 1]

        scan_ay = self.store_setting.iloc[2, 1]

        scan_fw = self.store_setting.iloc[3, 1]

        scan_ss = self.store_setting.iloc[4, 1]

        scan_tops = self.store_setting.iloc[5, 1]

        scan_dress = self.store_setting.iloc[6, 1]

        scan_carded = self.store_setting.iloc[7, 1]

        fw_season = self.store_setting.iloc[8, 1]

        ss_season = self.store_setting.iloc[9, 1]

        rolling_ss_fw = self.store_setting.iloc[10, 1]

        rolling_fw_ss = self.store_setting.iloc[11, 1]

        carded_1 = self.store_setting.iloc[12, 1]

        carded_2 = self.store_setting.iloc[13, 1]

        carded_3 = self.store_setting.iloc[14, 1]

        top_1 = self.store_setting.iloc[15, 1]

        top_2 = self.store_setting.iloc[16, 1]

        top_3 = self.store_setting.iloc[17, 1]

        dress_1 = self.store_setting.iloc[18, 1]

        dress_2 = self.store_setting.iloc[19, 1]

        dress_3 = self.store_setting.iloc[20, 1]

        return_pecentage = self.store_setting.iloc[21, 1]

        # SQL code for no scans below

        if in_season == 1:

            max_date = psql.read_sql('select max(date) from delivery', self.connection)
            max_date = max_date.iloc[0, 0]
            min_date = max_date - timedelta(days=21)

            no_scan = psql.read_sql(f"""

                /*select all of the items that have 0 sales  from the the sd_combo table*/

                SELECT store, item_group_desc 
                FROM SD_COMBO 
                WHERE (season = 'AY' or season = 'SS') and 
                       total_sales = 0 

                /*except statement is taking out any item that are duplicates*/
                EXCEPT

                    /*Select statement is grabbing all of the items that have been shipped in the past 3 weeks*/

                    select store, item_group_desc from delivery
                    inner join item_support on delivery.upc = item_support.upc
                    where date <= '{max_date}' and date >= '{min_date}'

                order by store

                """, self.connection)

        elif initial_display == 1:

            # if statement below is determing the different combinations for seasons

            if scan_ay == 1:

                if scan_fw == 1:  # ay = 1, fw=1

                    if scan_ss == 1:  # ay1 fw1 ss1
                        season = "(season = 'AY' or season = 'FW' or season = 'SS')"

                    else:  # ay1 fw1 ss0
                        season = "(season = 'AY' or season = 'FW')"

                else:  # ay = 1, fw = 0

                    if scan_ss == 1:  # ay1 fw0 ss1
                        season = "(season = 'AY' or season = 'SS')"

                    else:  # ay1 fw0 ss0
                        season = "(season = 'AY')"

            else:  # ay= 0

                if scan_fw == 1:  # ay0 fw1

                    if scan_ss == 1:  # ay0 fw1 ss1
                        season = "(season = 'FW' or season = 'SS')"

                    else:  # ay0 fw1 ss0
                        season = "(season = 'FW')"

                else:  # ay = 0, fw = 0

                    if scan_ss == 1:  # ay0 fw0 ss1
                        season = "(season = 'SS')"

                    else:  # ay0 fw0 ss0

                        print("\n\n\nALL SEASONS ON NO SCAN SETTINGS ARE SET TO 0\n\n\n")

            # determining different combinations for display size

            if scan_tops == 1:

                if scan_dress == 1:  # top1 dress1

                    if scan_carded == 1:  # top1 dress1 carded1

                        display_size = """(display_size = 'Long Hanging Top' or 
                                            display_size = 'Long Hanging Dress' or 
                                            display_size = 'Carded')"""

                    else:  # top1 dress1 carded0

                        display_size = "(display_size = 'Long Hanging Top' or display_size = 'Long Hanging Dress')"

                else:  # top1 dress0

                    if scan_carded == 1:  # top1 dress0  carded 1

                        display_size = "(display_size = 'Long Hanging Top' or display_size = 'Carded')"

                    else:  # top1 dress0  carded 0

                        display_size = "(display_size = 'Long Hanging Top')"

            else:

                if scan_dress == 1:  # top0 dress1

                    if scan_carded == 1:  # top0 dress1 carded1

                        display_size = """(display_size = 'Long Hanging Dress' or display_size = 'Carded')"""

                    else:  # top0 dress1 carded0

                        display_size = "(display_size = 'Long Hanging Dress')"

                else:  # top0 dress0

                    if scan_carded == 1:  # top0 dress0  carded 1

                        display_size = "(display_size = 'Carded')"

                    else:  # top0 dress0  carded 0

                        print('\n\n\nNO DISPLAY SIZE SELECTED. SET SETTING IN STORE SETTINGS FILE\n\n\n')

            no_scan = psql.read_sql(f"""

            with long_hanging as (
            select * from sd_combo 
            where ({display_size} and {season})),

            total as (
            select store, sum(total_sales)as sales from long_hanging
            group by store)

            select distinct store, sales from total
            where sales = 0

            """, self.connection)

        return no_scan

