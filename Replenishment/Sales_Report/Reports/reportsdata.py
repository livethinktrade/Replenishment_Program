import psycopg2
import pandas as pd
import pandas.io.sql as psql
from datetime import timedelta, date
import numpy as np
import datetime as dt
import DbConfig


class ReportsData:

    def __init__(self, store_type_input, store_setting):

        self.store_type_input = store_type_input

        self.store_setting = store_setting

        self.fw_season = self.store_setting.loc['FW_Season', 'values']
        self.ss_season = self.store_setting.loc['SS_Season', 'values']
        self.rolling_ss_fw = self.store_setting.loc['Rolling_SS_FW', 'values']
        self.rolling_fw_ss = self.store_setting.loc['Rolling_FW_SS', 'values']
        self.transition_year = self.store_setting.loc['Transition_year', 'values']
        self.transition_season = self.store_setting.loc['Transition_Season', 'values']


        self.connection = psycopg2.connect(database=f"test",
                                           user="postgres",
                                           password="winwin",
                                           host="localhost")

        if store_type_input in ['kroger_dallas',
                                'kroger_central',
                                'kroger_delta',
                                'kroger_michigan',
                                'kroger_columbus',
                                'kroger_atlanta',
                                'kroger_cincinatti',
                                'kroger_dillons',
                                'kroger_king_soopers',
                                'kroger_louisville',
                                'kroger_nashville',
                                'acme',
                                'texas_division']:

            self.week = 'store_week'
        elif store_type_input in ['intermountain', 'kvat', 'safeway_denver', 'jewel', 'sal','midwest','follett']:
            self.week = 'current_week'

        else:
            print('\n Store has not been established in list')

        with DbConfig.EnginePoolDB() as connection:

            # finds the current year per winwin company year
            year = psql.read_sql(f"select max(current_year) from {store_type_input}.sales2;", connection)

            self.store_year = year.iloc[0, 0]

            # finds the current week the store is on

            max_date = psql.read_sql(f"select max(date) from {store_type_input}.sales2 where store_year = {self.store_year}", connection)
            max_date = max_date.iloc[0, 0]

            week_num = psql.read_sql(f"select store_week from {store_type_input}.sales2 where date = '{max_date}'", connection)
            self.week_num = week_num.iloc[0,0]

            """ 
            This line is neccesary for jewel because when the sales data comes in on monday it usually
            includes sales that were made the previous day on Sunday. This is a problem because
            Sunday marks the beginning of the new week which meesses up the weekly reporting
            
            """

            if store_type_input in ['jewel','sal','midwest']:
                self.week_num -= 1

            """
            
            finds the number of weeks the store has been active in the current year.
            more important for new stores rather than old.
            helps calculate the avg store sales
            
            """

            num = psql.read_sql(f"select distinct({self.week}) from {self.store_type_input}.sales2 where store_year = {self.store_year}", connection)
            self.num = len(num)

    def sales_table(self):

        """ new sales table incorporating new support sheet"""

        sales_sql = f"""

        with

            sales_table as (


                SELECT distinct sales2.id,
                    sales2.transition_year,
                    sales2.transition_season,
                    sales2.store_year,
                    sales2.store_week,
                    sales2.store_number,
                    sales2.upc AS upc_11_digit,
                    sales2.sales,
                    sales2.qty,
                    sales2.current_year,
                    sales2.current_week,
                    sales2.store_type,
                    item_support2.season,
                    item_support2.category,
                    item_support2.upc,
                    item_support2.display_size,
                    item_support2.case_size,
                    item_support2.item_group_desc
                FROM {self.store_type_input}.sales2
                inner JOIN item_support2 ON sales2.code = item_support2.code),

            date as (

                select distinct store_number
                from sales_table
                order by store_number asc),


            current_week as (

                select store_number, store_year, {self.week}, sum(sales) as sales
                from sales_table
                where store_year = {self.store_year} and
                    {self.week} = {self.week_num} and
                    (season = 'FW' or season = 'AY' or season = 'SS') and
                    (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                group by store_number, store_year, {self.week}

                order by store_number asc),


            previous_week as (

                select store_number, store_year, {self.week}, sum(sales) as sales
                from sales_table

                where store_year = {self.store_year} and
                    {self.week} = {self.week_num}-1 and
                    (category != 'GM' or category != 'Accessory' or category != 'Scrub') and
                    (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                group by store_number, store_year, {self.week}

                order by store_number asc),

            previous_year_week as (

                select store_number, store_year, {self.week}, sum(sales) as sales
                from sales_table

                where store_year = {self.store_year}-1 and
                    {self.week} = {self.week_num} and
                    (season = 'FW' or season = 'AY' or season = 'SS') and
                    (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                group by store_number, store_year, {self.week}

                order by store_number asc),

            current_ytd_week as (

                select store_number, store_year, sum(sales) as sales
                from sales_table

                where store_year = {self.store_year} and
                    {self.week} <= {self.week_num} and
                    (season = 'FW' or season = 'AY' or season = 'SS') and
                    (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                group by store_number, store_year

                order by store_number asc),


            previous_ytd_week as (


                select store_number, store_year, sum(sales) as sales
                from sales_table
                /*looks for previous year so maybe max -1 when finding innitial start for python program*/
                where store_year = {self.store_year}-1 and
                    {self.week} <= {self.week_num} and
                    (season = 'FW' or season = 'AY' or season = 'SS') and
                    (category != 'GM' and category != 'Accessory' and category != 'Scrub')

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

        """new ytd mask update using the updated item_support sheet"""

        sales_sql_YTD_WoMask = f"""

        with

            sales_table as (

                         SELECT distinct sales2.id,
                            sales2.transition_year,
                            sales2.transition_season,
                            sales2.store_year,
                            sales2.store_week,
                            sales2.store_number,
                            sales2.upc AS upc_11_digit,
                            sales2.sales,
                            sales2.qty,
                            sales2.current_year,
                            sales2.current_week,
                            sales2.store_type,
                            item_support2.season,
                            item_support2.category,
                            item_support2.upc,
                            item_support2.display_size,
                            item_support2.case_size,
                            item_support2.item_group_desc
                         FROM {self.store_type_input}.sales2
                         inner JOIN item_support2 ON sales2.code = item_support2.code),

            store_count as (

                        select distinct store_number
                        from sales_table
                        where store_year = {self.store_year}),

            current_ytd_week as (

                        select store_number, store_year, sum(sales) as sales
                        from sales_table
                        where store_year = {self.store_year} and
                            {self.week} <= {self.week_num} and
                            (season = 'FW' or season = 'AY' or season = 'SS') and
                            (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                        group by store_number, store_year

                        order by store_number asc),

            previous_ytd_week as (


                        select store_number, store_year, sum(sales) as sales
                        from sales_table

                        /*looks for previous year so maybe max -1 when finding innitial start for python program*/
                        where store_year = {self.store_year}-1 and
                            {self.week} <= {self.week_num} and
                            (season = 'FW' or season = 'AY' or season = 'SS') and
                            (category != 'GM' and category != 'Accessory' and category != 'Scrub')

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

        """new ytd with mask using the new support sheet"""

        sales_sql_YTD_Mask = f"""

        with

            sales_table as (

                         SELECT distinct sales2.id,
                            sales2.transition_year,
                            sales2.transition_season,
                            sales2.store_year,
                            sales2.store_week,
                            sales2.store_number,
                            sales2.upc AS upc_11_digit,
                            sales2.sales,
                            sales2.qty,
                            sales2.current_year,
                            sales2.current_week,
                            sales2.store_type,
                            item_support2.season,
                            item_support2.category,
                            item_support2.upc,
                            item_support2.display_size,
                            item_support2.case_size,
                            item_support2.item_group_desc
                         FROM {self.store_type_input}.sales2
                         inner JOIN item_support2 ON sales2.code = item_support2.code),

            store_count as (

                        select distinct store_number
                        from sales_table
                        where store_year = {self.store_year}),

            current_ytd_week as (

                        select store_number, store_year, sum(sales) as sales
                        from sales_table
                        where store_year = {self.store_year} and
                            {self.week} <= {self.week_num}

                        group by store_number, store_year

                        order by store_number asc),

            previous_ytd_week as (


                        select store_number, store_year, sum(sales) as sales
                        from sales_table

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

        """new item sales rank"""

        item_sales_rank = f"""

        with

            sales_table as (


                SELECT distinct sales2.id,
                    sales2.transition_year,
                    sales2.transition_season,
                    sales2.store_year,
                    sales2.store_week,
                    sales2.store_number,
                    sales2.upc AS upc_11_digit,
                    sales2.sales,
                    sales2.qty,
                    sales2.current_year,
                    sales2.current_week,
                    sales2.store_type,
                    item_support2.season,
                    item_support2.category,
                    item_support2.upc,
                    item_support2.display_size,
                    item_support2.case_size,
                    item_support2.item_group_desc
                FROM {self.store_type_input}.sales2
                inner JOIN item_support2 ON sales2.code = item_support2.code),

            year_total as(
                        Select sum(sales) as year_sales, sum(qty) as year_total_unit
                        from sales_table
                        where store_year = {self.store_year}),

            item_sales_rank as (
                        Select item_group_desc, round(sum(sales),2) as sales, sum(qty) as units_sold,season
                        from sales_table
                        where store_year = {self.store_year}
                        group by item_group_desc, season
                        order by sum(sales) desc)

        select item_group_desc,
                sales,
                units_sold,
                round((sales/year_sales),3) as percent_of_total_sales,
                season
            from item_sales_rank, year_total

        """


        # this db_infrastructure statement prduces a query that shows all of the items that was sold for a given year.
        # Provides a table with item group desc, season, sum sales, sum qty, % of total sales


        rank = psql.read_sql(f'{item_sales_rank}', self.connection)

        i = 0

        # using the previous query, python selects the item group desc and finds how many stores carried that particular item during
        # that time. Columns will consist of item_group_desc,sales,units_sold,percent_of_total_sales,season,active,stores,sales per active store

        while i < len(rank):
            item = rank.loc[i, 'item_group_desc']

            store_count = f"""

            with

            sales_table as (


                SELECT distinct sales2.id,
                    sales2.transition_year,
                    sales2.transition_season,
                    sales2.store_year,
                    sales2.store_week,
                    sales2.store_number,
                    sales2.upc AS upc_11_digit,
                    sales2.sales,
                    sales2.qty,
                    sales2.current_year,
                    sales2.current_week,
                    sales2.store_type,
                    item_support2.season,
                    item_support2.category,
                    item_support2.upc,
                    item_support2.display_size,
                    item_support2.case_size,
                    item_support2.item_group_desc
                FROM {self.store_type_input}.sales2
                inner JOIN item_support2 ON sales2.code = item_support2.code),


                store_with_item as (

                select * from sales_table where item_group_desc = '{item}' and store_year = {self.store_year})

            select count(distinct(store_number)) from store_with_item

            """

            store_count = psql.read_sql(f'{store_count}', self.connection)

            store_count = store_count.iloc[0, 0]

            rank.loc[i, 'active stores'] = store_count

            i += 1

        rank['sales per active store'] = round(rank['sales'] / rank['active stores'], 2)

        rank['active stores'] = rank['active stores'].astype(int)

        rank = rank.sort_values(by='sales per active store', ascending=False)

        rank = rank[['item_group_desc', 'sales', 'sales per active store', 'active stores', 'percent_of_total_sales']]

        item_sales_rank = rank.head(10)


        return item_sales_rank

    def on_hands(self):

        """On Hands For Each Item in Each store based on Transition Season"""

        # finding the max date so I can find the current year.
        # Current year needed to we can see set a point of ref to subtract for ay items
        max_year = psql.read_sql(f"select max(date) from {self.store_type_input}.delivery2", self.connection)

        max_year = max_year.iloc[0, 0]
        max_year = max_year.year

        min_year = max_year - self.store_setting.loc['AY_time', 'values']

        # verifying that only one transition setting is selected. also verifies that
        # no has messed with "sheet 2" in store setting file
        if (self.fw_season + self.ss_season + self.rolling_ss_fw + self.rolling_fw_ss) > 1:

            transition_setting = []

            if self.fw_season >= 1:
                transition_setting.append('fw_season')

            if self.ss_season >= 1:
                transition_setting.append('ss_season')

            if self.rolling_fw_ss >= 1:
                transition_setting.append('rolling_fw_ss')

            if self.rolling_ss_fw >= 1:
                transition_setting.append('rolling_ss_fw')

            raise Exception(
                f"More than one Transitions Store Setting selected({transition_setting}).Only one is allowed")

        # filter all items within the transition setting season
        # Season 1 is used to set for the current season Season 2 is used to filter out the previous season.

        if self.fw_season == 1:

            season1 = "FW"
            season2 = 'FW'
            sales_season2 = 'SS'
            transition_year1 = self.transition_year
            transition_year2 = self.transition_year
            transition_season1 = 'FW'
            transition_season2 = 'FW'

        elif self.ss_season == 1:

            season1 = "SS"
            season2 = 'SS'
            sales_season2 = 'FW'
            transition_year1 = self.transition_year
            transition_year2 = self.transition_year
            transition_season1 = 'SS'
            transition_season2 = 'SS'


        elif self.rolling_ss_fw == 1:

            season1 = "SS"
            season2 = 'FW'
            sales_season2 = 'FW'
            transition_year1 = self.transition_year
            transition_year2 = self.transition_year
            transition_season1 = 'SS'
            transition_season2 = 'FW'

        elif self.rolling_fw_ss == 1:

            season1 = "SS"
            season2 = 'FW'
            sales_season2 = 'FW'
            transition_year1 = self.transition_year
            transition_year2 = self.transition_year-1
            transition_season1 = 'SS'
            transition_season2 = 'FW'

        else:
            raise Exception("No Transition Store Setting has been selected")

        sql_query = f"""
        
        with

            delivery_table as (

                /*This table is basically the delivery table with the support sheet lined up */
                select id, transition_year, transition_season, delivery2.type, date, delivery2.upc, store,
                qty, season, category, item_group_desc, display_size, case_size
                from {self.store_type_input}.delivery2
                inner join item_support2 on {self.store_type_input}.delivery2.code = item_support2.code
                where date >= '01-01-{min_year}'),

            deliv_pivot_credit as (

                /*this next table will grab all of credits for ay items along with credits for whatever season we are in*/

                SELECT store,
                    item_group_desc,
                    sum(qty) AS credit,
                    display_size,
                    season,
                    case_size


                FROM delivery_table

                where
                        /*this next line tracks AY items will need to use python to dynamically track 2 years worth only*/
                    (

                        (
                            season = 'AY'
                            and type = 'Credit Memo'
                            and (category != 'Accessory' and category != 'GM' and category != 'Scrub' and category != 'Rack')
                        )

                         or

                        /*filtering out the current seasonal items */
                        (
                            season = '{season1}'
                            and type = 'Credit Memo'
                            and (category != 'Accessory' and category != 'GM' and category != 'Scrub' and category != 'Rack')
                            and transition_year = {transition_year1}
                            and transition_season = '{transition_season1}'
                        )

                        or
                        
                        /*this next section is only applicable if using rolling transition. will filter all of the prev seasonal items*/
                        (
                            season = '{season2}'
                            and type = 'Credit Memo'
                            and (category != 'Accessory' and category != 'GM' and category != 'Scrub' and category != 'Rack')
                            and transition_year = {transition_year2}
                            and transition_season = '{transition_season2}'
                        )

                      )

                group by store, item_group_desc,display_size,season, case_size
                order by store),

            deliv_pivot_invoice as (


                SELECT store,
                    item_group_desc,
                    sum(qty) AS deliveries,
                    display_size,
                    season,
                    case_size

                FROM delivery_table

                where
                        /*this next line tracks AY items will need to use python to dynamically track 2 years worth only*/
                    (

                        (
                            season = 'AY'
                            and (type = 'Invoice' or type = 'BandAid' or type = 'Reset')
                            and (category != 'Accessory' and category != 'GM' and category != 'Scrub' and category != 'Rack')
                        )

                         or

                        /*this next line should be dynamic using python keep in mind that rolling transition will include SS and FW items */
                        (
                            season = '{season1}'
                            and (type = 'Invoice' or type = 'BandAid' or type = 'Reset')
                            and (category != 'Accessory' and category != 'GM' and category != 'Scrub' and category != 'Rack')
                            and transition_year = {transition_year1}
                            and transition_season = '{transition_season1}'
                        )
                        
                        or
                        
                        (
                            season = '{season2}'
                            and (type = 'Invoice' or type = 'BandAid' or type = 'Reset')
                            and (category != 'Accessory' and category != 'GM' and category != 'Scrub' and category != 'Rack')
                            and transition_year = {transition_year2}
                            and transition_season = '{transition_season2}'
                        )

                      )

                group by store, item_group_desc,display_size,season, case_size
                order by store),

            delivery_pivot as (

                select deliv_pivot_invoice.store,

                        deliv_pivot_invoice.item_group_desc,
                        deliveries,
                        COALESCE(credit,0) as credit,
                        deliv_pivot_invoice.display_size,
                        deliv_pivot_invoice.season,
                        deliv_pivot_invoice.case_size
                from deliv_pivot_invoice
                full join deliv_pivot_credit on deliv_pivot_invoice.item_group_desc = deliv_pivot_credit.item_group_desc AND
                            deliv_pivot_invoice.store = deliv_pivot_credit.store),

            sales_table as (

                SELECT sales2.id,
                    sales2.transition_year,
                    sales2.transition_season,
                    sales2.store_year,
                    sales2.store_week,
                    sales2.store_number,
                    sales2.upc AS upc_11_digit,
                    sales2.sales,
                    sales2.qty,
                    sales2.current_year,
                    sales2.store_type,
                    item_support2.season,
                    item_support2.category,
                    item_support2.upc,
                    item_support2.display_size,
                    item_support2.case_size,
                    item_support2.item_group_desc
                FROM {self.store_type_input}.sales2
                inner JOIN item_support2 ON {self.store_type_input}.sales2.code = item_support2.code
                where date >= '01-01-{min_year}'),

            sales_pivot_AY as (

                SELECT store_number,
                        item_group_desc,
                        sum(qty) AS sales

                from sales_table
                where season = 'AY' and (category != 'Accessory' and category != 'GM' and category != 'Rack')
                GROUP BY store_number,item_group_desc
                ORDER BY store_number, item_group_desc),

            /*sales for the current season*/
            sales_pivot_current_season as (
                SELECT store_number,
                        item_group_desc,
                        sum(qty) AS sales
                FROM sales_table
                where season = '{season1}' and (category != 'Accessory' and category != 'GM' and category != 'Rack') and transition_year = {transition_year1} and transition_season = '{transition_season1}'
                GROUP BY store_number, item_group_desc
                ORDER BY store_number),

            /*sales for the previous season*/
            sales_pivot_previous_season as (

                SELECT store_number,
                        item_group_desc,
                        sum(qty) AS sales
                FROM sales_table
                where season = '{sales_season2}' and (category != 'Accessory' and category != 'GM' and category != 'Rack') and transition_year = {transition_year2} and transition_season = '{transition_season2}'
                GROUP BY store_number, item_group_desc
                ORDER BY store_number),

            combine_sd as (

                /*combines all of the sales tables and then joins it to the delivery  table*/

                select delivery_pivot.store,
                       delivery_pivot.item_group_desc,
                       delivery_pivot.season,
                       delivery_pivot.display_size,
                       delivery_pivot.case_size,
                       deliveries,
                       credit,
                       sales_pivot_AY.item_group_desc as AY_Desc,
                       sales_pivot_current_season.item_group_desc as FW_Desc,
                       sales_pivot_previous_season.item_group_desc as SS_Desc

                from delivery_pivot
                full join sales_pivot_current_season on sales_pivot_current_season.store_number = delivery_pivot.store and
                           sales_pivot_current_season.item_group_desc = delivery_pivot.item_group_desc
                full join sales_pivot_ay on sales_pivot_ay.store_number = delivery_pivot.store and
                           sales_pivot_ay.item_group_desc = delivery_pivot.item_group_desc
                full join sales_pivot_previous_season on sales_pivot_previous_season.store_number = delivery_pivot.store and
                           sales_pivot_previous_season.item_group_desc = delivery_pivot.item_group_desc

                group by
                        delivery_pivot.store,
                       delivery_pivot.item_group_desc,
                       delivery_pivot.season,
                       delivery_pivot.display_size,
                       delivery_pivot.case_size,
                       deliveries,
                       credit,
                       sales_pivot_AY.item_group_desc,
                       sales_pivot_current_season.item_group_desc,
                       sales_pivot_previous_season.item_group_desc


                order by delivery_pivot.store asc, item_group_desc asc),


       			bandaids as (
			
				select store_id, item_group_desc, sum(qty) as bandaid 
				from {self.store_type_input}.bandaids
				group by store_id, item_group_desc
				
			)
				


        select
               store,
               combine_sd.item_group_desc,
               display_size,
               season,
               case_size,
               deliveries,
               credit,
               
               coalesce(sales_pivot_previous_season.sales,0) 
                        + coalesce(sales_pivot_current_season.sales,0) 
                        + coalesce(sales_pivot_ay.sales,0
                        ) as total_sales,
                        
			   coalesce(bandaid,0) as bandaid,
			   
			   /*Calculating the on hands: OH = Delivery - Sales + Bandaids*/
	   			(
					/* The Delivery Variable in the equation composes of deliveries(aka invoices) and credit (aka credit memos)*/
					deliveries 
					+ credit 
					
					/* The Sales Variable in the equation is composed of Sales for AY, Sales from the currents season, and Sales from the previous season*/
					- coalesce(sales_pivot_current_season.sales,0) 
					- coalesce(sales_pivot_ay.sales,0)
					- coalesce(sales_pivot_previous_season.sales,0)
					
					/*Bandaid section will only concist of AY items*/

					+ coalesce(bandaid,0)
					
				) as on_hand,
			   
			   
               case
                    when (
						    deliveries 
						  + credit 
						  - coalesce(sales_pivot_current_season.sales,0) 
						  - coalesce(sales_pivot_ay.sales,0)
						  - coalesce(sales_pivot_previous_season.sales,0)
						  + coalesce(bandaid,0)
					
						  ) <= 0
						 
                        then 0
						 
                    when (
						
						  deliveries 
						+ credit 
						- coalesce(sales_pivot_current_season.sales,0) 
						- coalesce(sales_pivot_ay.sales,0)
						- coalesce(sales_pivot_previous_season.sales,0)
						+ coalesce(bandaid,0)
					
					    ) > 0
						
                        
						then round(
                                    (
                                        (
                                            deliveries 
                                            + credit 
                                            - coalesce(sales_pivot_current_season.sales,0) 
                                            - coalesce(sales_pivot_ay.sales,0)
                                            - coalesce(sales_pivot_previous_season.sales,0)
                                            + coalesce(bandaid,0)

                                        )/case_size
                                    ),2
                                   )
                    end as case_qty
					
        from combine_sd
        full join sales_pivot_current_season on sales_pivot_current_season.store_number = combine_sd.store and
                   sales_pivot_current_season.item_group_desc = combine_sd.item_group_desc
        full join sales_pivot_ay on sales_pivot_ay.store_number = combine_sd.store and
                   sales_pivot_ay.item_group_desc = combine_sd.item_group_desc
        full join sales_pivot_previous_season on sales_pivot_previous_season.store_number = combine_sd.store and
                   sales_pivot_previous_season.item_group_desc = combine_sd.item_group_desc
	   	full join bandaids on bandaids.store_id = combine_sd.store and
                   bandaids.item_group_desc = combine_sd.item_group_desc

        order by store asc, item_group_desc asc
        
        """

        on_hand = psql.read_sql(sql_query, self.connection)

        on_hand = on_hand.dropna()
        on_hand = on_hand.sort_values(by=['store', 'display_size', 'case_qty'])
        on_hand['store'] = on_hand['store'].astype(int)
        on_hand = on_hand.reset_index(drop=True)

        return on_hand

    def no_scan(self, on_hands):

        """ new no scans incorporating new sd_combo df and itemsupport sheet

            notes in the future:

            program produces a df. df depends on the type of no scan being executed (either in_season or initial_display)

            1) in season will produce a df with columns: store, item_group_desc, last shipped, weeks age

            2) no scans will produce  a df with columns: store, total_sales

            """

        # this variable will be used for the initial display no scan if needed
        on_hand = on_hands
        on_hands = on_hands[on_hands['total_sales'] == 0]

        # Assigning variable from store seeting sheet 2 table.

        initial_display = self.store_setting.loc['Initial_Display', 'values']

        in_season = self.store_setting.loc['In_Season', 'values']

        scan_ay = self.store_setting.loc['Scan_AY', 'values']

        scan_fw = self.store_setting.loc['Scan_FW', 'values']

        scan_ss = self.store_setting.loc['Scan_SS', 'values']

        scan_tops = self.store_setting.loc['Scan_Tops', 'values']

        scan_dress = self.store_setting.loc['Scan_Dress', 'values']

        scan_carded = self.store_setting.loc['Scan_Carded', 'values']

        # no scans for in_season

        if in_season == 1:

            # no scans for in_season

            max_date = psql.read_sql(f'select max(date) from {self.store_type_input}.delivery2', self.connection)
            max_date = max_date.iloc[0, 0]
            min_date = max_date - timedelta(days=21)

            recently_shipped = psql.read_sql(f"""

                select distinct(item_group_desc), store
                from {self.store_type_input}.delivery2
                inner join item_support2 on {self.store_type_input}.delivery2.code = item_support2.code
                where date <= '{max_date}' and date >= '{min_date}'
                order by store
                """, self.connection)

            # finds the items that are on the 0 sales df list and the recently shipped list
            recently_shipped = recently_shipped[['store', 'item_group_desc']]
            int_df = pd.merge(on_hands, recently_shipped, how='inner', on=['store', 'item_group_desc'])

            on_hands = on_hands.set_index(['store', 'item_group_desc'])

            # deltes recently shipped items off of the 0 sales list
            i = 0
            while i < len(int_df):
                store = int_df.loc[i, 'store']
                item_group_desc = int_df.loc[i, 'item_group_desc']
                on_hands = on_hands.drop((store, item_group_desc))
                i += 1
            on_hands = on_hands.reset_index()

            on_hands = on_hands[['store', 'item_group_desc']]

            i = 0
            while i < len(on_hands):
                store = on_hands.loc[i, 'store']
                item_group_desc = on_hands.loc[i, 'item_group_desc']

                last_date_shipped = psql.read_sql(f"""

                    select max(date)
                    from {self.store_type_input}.delivery2
                    inner join item_support2 on {self.store_type_input}.delivery2.code = item_support2.code
                    WHERE store = {store} and item_group_desc = '{item_group_desc}'
                    """, self.connection)

                last_date_shipped = last_date_shipped.iloc[0, 0]

                on_hands.loc[i, 'last shipped'] = last_date_shipped

                i += 1

            today = date.today()
            on_hands['weeks age'] = (today - on_hands['last shipped'])
            on_hands['weeks age'] = (on_hands['weeks age'] / np.timedelta64(1, 'D')).astype(int)
            on_hands['weeks age'] = (round(on_hands['weeks age'] / 7)).astype(int)

        # no scans for initial display
        elif initial_display == 1:

            # if statement below is determing the different combinations for seasons

            if scan_ay == 1:

                if scan_fw == 1:  # ay = 1, fw=1

                    if scan_ss == 1:  # ay1 fw1 ss1
                        season = (on_hands['season'] == 'AY') | (on_hands['season'] == 'SS') | (
                                    on_hands['season'] == 'FW')

                    else:  # ay1 fw1 ss0
                        season = (on_hands['season'] == 'AY') | (on_hands['season'] == 'FW')

                else:  # ay = 1, fw = 0

                    if scan_ss == 1:  # ay1 fw0 ss1
                        season = (on_hands['season'] == 'AY') | (on_hands['season'] == 'SS')

                    else:  # ay1 fw0 ss0
                        season = (on_hands['season'] == 'AY')

            else:  # ay= 0

                if scan_fw == 1:  # ay0 fw1

                    if scan_ss == 1:  # ay0 fw1 ss1
                        season = (on_hands['season'] == 'SS') | (on_hands['season'] == 'FW')

                    else:  # ay0 fw1 ss0
                        season = (on_hands['season'] == 'FW')

                else:  # ay = 0, fw = 0

                    if scan_ss == 1:  # ay0 fw0 ss1
                        season = (on_hands['season'] == 'SS')

                    else:  # ay0 fw0 ss0

                        print("\n\n\nALL SEASONS ON NO SCAN SETTINGS ARE SET TO 0\n\n\n")

            # determining different combinations for display size

            if scan_tops == 1:

                if scan_dress == 1:  # top1 dress1

                    if scan_carded == 1:  # top1 dress1 carded1

                        display_size = (on_hands['display_size'] == 'Carded') | (
                                    on_hands['display_size'] == 'Long Hanging Dress') | (
                                                   on_hands['display_size'] == 'Long Hanging Top')

                    else:  # top1 dress1 carded0

                        display_size = (on_hands['display_size'] == 'Long Hanging Dress') | (
                                    on_hands['display_size'] == 'Long Hanging Top')

                else:  # top1 dress0

                    if scan_carded == 1:  # top1 dress0  carded 1

                        display_size = (on_hands['display_size'] == 'Carded') | (
                                    on_hands['display_size'] == 'Long Hanging Top')

                    else:  # top1 dress0  carded 0

                        display_size = (on_hands['display_size'] == 'Long Hanging Top')

            else:

                if scan_dress == 1:  # top0 dress1

                    if scan_carded == 1:  # top0 dress1 carded1

                        display_size = (on_hands['display_size'] == 'Carded') | (
                                    on_hands['display_size'] == 'Long Hanging Dress')

                    else:  # top0 dress1 carded0

                        display_size = (on_hands['display_size'] == 'Long Hanging Dress')

                else:  # top0 dress0

                    if scan_carded == 1:  # top0 dress0  carded 1

                        display_size = (on_hands['display_size'] == 'Carded')

                    else:  # top0 dress0  carded 0

                        print('\n\n\nNO DISPLAY SIZE SELECTED. SET SETTING IN STORE SETTINGS FILE\n\n\n')


        if initial_display == 1:

            # apply filter per settings need to fix code below
            # on_hands = on_hand.loc[display_size & season]

            on_hands = on_hand

            # select only needed columns
            on_hands = on_hands[['store', 'total_sales']]

            # group by function using the store numbers. This will group the total of all sales after filtering.
            on_hands = on_hands.groupby(by=["store"]).sum()

            # reset the index
            on_hands = on_hands.reset_index(drop=False)

            # filter all stores that have 0 sales
            # this would show that they have not sold at least one of the items from any of the cases taht have been shipped
            on_hands = on_hands[on_hands['total_sales'] == 0]


        return on_hands

    def item_approval(self):

        item_approval = f"""
        
        select season, category, style, 
                display_size, item_group_desc, 
                on_hand as inventory_on_hand,
                round((on_hand/item_support2.case_size),0) as num_of_cases,
                store_price from {self.store_type_input}.item_approval

        inner join item_support2 on item_approval.code = item_support2.code
        inner join inventory on item_approval.code = inventory.code
        where store_price < 999
        order by display_size, on_hand desc
        
        """

        item_approval = psql.read_sql(f'{item_approval}', self.connection)

        return item_approval

    def store_sales_rank(self):

        store_sales_rank = f"""
        
        select store_number, round(sum(sales),0) as YTD_Sales from {self.store_type_input}.sales2
        where current_year = {self.store_year}
        group by store_number
        order by sum(sales) desc

        """

        store_sales_rank = psql.read_sql(f'{store_sales_rank}', self.connection)

        return  store_sales_rank

    def store_program(self):

        store_program = f"""
        
        select store_program.store_id, 
                cd_ay,cd_sn,lht_ay, 
                lht_sn,lhd_ay, lhd_sn,
                lhp_ay, lhp_sn, total_cases, notes
        from {self.store_type_input}.store_program
        inner join master_planogram on {self.store_type_input}.store_program.program_id = master_planogram.program_id
        inner join {self.store_type_input}.store on {self.store_type_input}.store_program.store_id = {self.store_type_input}.store.store_id
        order by {self.store_type_input}.store_program.store_id
        
        """

        store_program = psql.read_sql(f'{store_program}', self.connection)

        store_program = store_program.groupby(['store_id','notes']).sum()

        store_program = store_program.reset_index()

        store_program = store_program[['store_id', 'cd_ay','cd_sn','lht_ay',
                                        'lht_sn', 'lhd_ay', 'lhd_sn', 'lhp_ay',
                                       'lhp_sn', 'total_cases', 'notes']]

        i = 0

        store_program['programs'] = 0

        while i < len(store_program):

            store = store_program.loc[i, 'store_id']

            programs = psql.read_sql(f'select * from {self.store_type_input}.store_program where store_id = {store}', self.connection)

            programs = programs[['program_id']]

            programs = programs.squeeze()

            try:

                programs = programs.str.cat(sep=', ')

            except:

                pass

            store_program.loc[i, 'programs'] = programs

            i += 1

        return store_program

    def ghost_inventory(self):

        reports = ReportsData(self.store_type_input, self.store_setting)

        on_hands = reports.on_hands()

        inventory_age = on_hands

        i = 0

        with DbConfig.EnginePoolDB() as connection:

            while i < len(on_hands):

                store = on_hands.loc[i, 'store']

                item_group_desc = on_hands.loc[i, 'item_group_desc']

                # grab the last date the item was sold

                date = psql.read_sql(f'''
    
                select store_year, date from {self.store_type_input}.sales2
                inner join item_support2 on {self.store_type_input}.sales2.code = item_support2.code
                where store_number = {store} and item_group_desc = '{item_group_desc}'
                order by date desc
    
                ''', connection)

                try:

                    last_ship_date = date.loc[0, 'date']
                    last_ship_year = date.loc[0, 'store_year']

                    inventory_age.loc[i, 'last sale date'] = last_ship_date
                    inventory_age.loc[i, 'last sale year'] = last_ship_year

                except KeyError:

                    inventory_age.loc[i, 'last sale date'] = 'No Sales'
                    inventory_age.loc[i, 'last sale year'] = 'No Sales'

                # grabs the last date the item was shipped

                date = psql.read_sql(f'''
    
                select store, date, item_group_desc from {self.store_type_input}.delivery2
                inner join item_support2 on {self.store_type_input}.delivery2.code = item_support2.code
                where store = {store} and item_group_desc = '{item_group_desc}'
                order by date desc
    
    
                ''', connection)

                last_ship_week = date.loc[0, 'date']

                inventory_age.loc[i, 'last delivery date'] = last_ship_week

                i += 1

        store_program = reports.store_program()

        store_program = store_program.set_index('store_id')


        store_program['Carded']= store_program['cd_ay'] + store_program['cd_sn']
        store_program['Long Hanging Top']= store_program['lht_ay'] + store_program['lht_sn']
        store_program['Long Hanging Dress']= store_program['lhd_ay'] + store_program['lhd_sn']
        store_program['Long Hanging Pant']= store_program['lhp_ay'] + store_program['lhp_sn']


        i = 0

        while i < len(inventory_age):

            try:

                store = inventory_age.loc[i, 'store']
                display_size = inventory_age.loc[i, 'display_size']

                allocated_space = store_program.loc[store, display_size]

                inventory_age.loc[i, 'allocated space'] = allocated_space

            except KeyError:

                inventory_age.loc[i, 'allocated space'] = 0

            i += 1

        inventory_age['% of display space'] = (inventory_age['case_qty'] / inventory_age['allocated space'])

        # inventory_age = inventory_age[inventory_age['season'] == 'AY']

        return inventory_age

    def sales_table_qty(self):

        sales_sql = f"""

               with

                   sales_table as (


                       SELECT distinct sales2.id,
                           sales2.transition_year,
                           sales2.transition_season,
                           sales2.store_year,
                           sales2.store_week,
                           sales2.store_number,
                           sales2.upc AS upc_11_digit,
                           sales2.sales,
                           sales2.qty,
                           sales2.current_year,
                           sales2.current_week,
                           sales2.store_type,
                           item_support2.season,
                           item_support2.category,
                           item_support2.upc,
                           item_support2.display_size,
                           item_support2.case_size,
                           item_support2.item_group_desc
                       FROM {self.store_type_input}.sales2
                       inner JOIN item_support2 ON sales2.code = item_support2.code),

                   date as (

                       select distinct store_number
                       from sales_table
                       order by store_number asc),


                   current_week as (

                       select store_number, store_year, {self.week}, sum(qty) as sales
                       from sales_table
                       where store_year = {self.store_year} and
                           {self.week} = {self.week_num} and
                           (season = 'FW' or season = 'AY' or season = 'SS') and
                           (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                       group by store_number, store_year, {self.week}

                       order by store_number asc),


                   previous_week as (

                       select store_number, store_year, {self.week}, sum(qty) as sales
                       from sales_table

                       where store_year = {self.store_year} and
                           {self.week} = {self.week_num}-1 and
                           (category != 'GM' or category != 'Accessory' or category != 'Scrub') and
                           (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                       group by store_number, store_year, {self.week}

                       order by store_number asc),

                   previous_year_week as (

                       select store_number, store_year, {self.week}, sum(qty) as sales
                       from sales_table

                       where store_year = {self.store_year}-1 and
                           {self.week} = {self.week_num} and
                           (season = 'FW' or season = 'AY' or season = 'SS') and
                           (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                       group by store_number, store_year, {self.week}

                       order by store_number asc),

                   current_ytd_week as (

                       select store_number, store_year, sum(qty) as sales
                       from sales_table

                       where store_year = {self.store_year} and
                           {self.week} <= {self.week_num} and
                           (season = 'FW' or season = 'AY' or season = 'SS') and
                           (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                       group by store_number, store_year

                       order by store_number asc),


                   previous_ytd_week as (


                       select store_number, store_year, sum(qty) as sales
                       from sales_table
                       /*looks for previous year so maybe max -1 when finding innitial start for python program*/
                       where store_year = {self.store_year}-1 and
                           {self.week} <= {self.week_num} and
                           (season = 'FW' or season = 'AY' or season = 'SS') and
                           (category != 'GM' and category != 'Accessory' and category != 'Scrub')

                       group by store_number, store_year

                       order by store_number asc)

               select
                   date.store_number,
                   current_week.sales as current_week,
                   previous_week.sales as previous_week,
                   /*WOW sales percentage */
                   case
                       when current_week.sales < 0 or previous_week.sales <= 0
                           then NULL
                       when current_week.sales >= 0 or previous_week.sales > 0
                           then ((current_week.sales-previous_week.sales)/(previous_week.sales))
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

    def item_sales_rank_qty(self):

        """Generates a table that gives you the qty sold for each item group desc ranked"""

        item_sales_rank = f"""

        with

            sales_table as (


                SELECT distinct sales2.id,
                    sales2.transition_year,
                    sales2.transition_season,
                    sales2.store_year,
                    sales2.store_week,
                    sales2.store_number,
                    sales2.upc AS upc_11_digit,
                    sales2.sales,
                    sales2.qty,
                    sales2.current_year,
                    sales2.current_week,
                    sales2.store_type,
                    item_support2.season,
                    item_support2.category,
                    item_support2.upc,
                    item_support2.display_size,
                    item_support2.case_size,
                    item_support2.item_group_desc
                FROM {self.store_type_input}.sales2
                inner JOIN item_support2 ON sales2.code = item_support2.code),

            year_total as(
                        Select sum(sales) as year_sales, sum(qty) as year_total_unit
                        from sales_table
                        where store_year = {self.store_year}),

            item_sales_rank as (
                        Select item_group_desc, round(sum(sales),2) as sales, sum(qty) as units_sold,season
                        from sales_table
                        where store_year = {self.store_year}
                        group by item_group_desc, season
                        order by sum(sales) desc)

        select item_group_desc,
                sales,
                units_sold,
                round((sales/year_sales),3) as percent_of_total_sales,
                season
            from item_sales_rank, year_total

        """


        # this db_infrastructure statement prduces a query that shows all of the items that was sold for a given year.
        # Provides a table with item group desc, season, sum sales, sum qty, % of total sales


        rank = psql.read_sql(f'{item_sales_rank}', self.connection)

        i = 0

        # using the previous query, python selects the item group desc and finds how many stores carried that particular item during
        # that time. Columns will consist of item_group_desc,sales,units_sold,percent_of_total_sales,season,active,stores,sales per active store

        while i < len(rank):
            item = rank.loc[i, 'item_group_desc']

            store_count = f"""

            with

            sales_table as (


                SELECT distinct sales2.id,
                    sales2.transition_year,
                    sales2.transition_season,
                    sales2.store_year,
                    sales2.store_week,
                    sales2.store_number,
                    sales2.upc AS upc_11_digit,
                    sales2.sales,
                    sales2.qty,
                    sales2.current_year,
                    sales2.current_week,
                    sales2.store_type,
                    item_support2.season,
                    item_support2.category,
                    item_support2.upc,
                    item_support2.display_size,
                    item_support2.case_size,
                    item_support2.item_group_desc
                FROM {self.store_type_input}.sales2
                inner JOIN item_support2 ON sales2.code = item_support2.code),


                store_with_item as (

                select * from sales_table where item_group_desc = '{item}' and store_year = {self.store_year})

            select count(distinct(store_number)) from store_with_item

            """

            store_count = psql.read_sql(f'{store_count}', self.connection)

            store_count = store_count.iloc[0, 0]

            rank.loc[i, 'active stores'] = store_count

            i += 1

        rank['units sold per active store'] = round(rank['units_sold'] / rank['active stores'], 2)

        rank['active stores'] = rank['active stores'].astype(int)

        rank = rank.sort_values(by='units sold per active store', ascending=False)

        rank = rank[['item_group_desc', 'units_sold', 'units sold per active store', 'active stores', 'percent_of_total_sales']]

        item_sales_rank = rank.head(10)


        return item_sales_rank

    def kroger_division_sales(self):

        '''

        :return: df

        returns a table that concist of sales $ for each store for a particular division.
        table contains columns such:

        ytd_sales.store_number, ytd_dollar, ytd_qty, current_wk_sales, prev_wk_sales from ytd_sales

        '''

        with DbConfig.EnginePoolDB() as connection:

            sales_report = psql.read_sql(f"""
    
                    with
                        ytd_sales as (
                                select store_number, round(sum(sales)) as ytd_dollar, sum(qty) as ytd_qty
                                from {self.store_type_input}.sales2
                                where store_year = {self.store_year}
                                group by store_number
                                order by store_number
                                ),
    
                        current_wk_sales as (
                                select store_number,  round(sum(sales)) as current_wk_sales
                                from {self.store_type_input}.sales2
                                where store_year = {self.store_year} and store_week = {self.week_num}
                                group by store_number
                                order by store_number
                                ),
    
                        prev_wk_sales as (
                                select store_number, round(sum(sales)) as prev_wk_sales
                                from {self.store_type_input}.sales2
                                where store_year = {self.store_year} and store_week = {self.week_num}-1
                                group by store_number
                                order by store_number
                        )
    
                    select ytd_sales.store_number, ytd_dollar as ytd_dollar, ytd_qty, 
                           current_wk_sales, prev_wk_sales from ytd_sales
                    left join current_wk_sales on ytd_sales.store_number = current_wk_sales.store_number
                    left join prev_wk_sales on ytd_sales.store_number =  prev_wk_sales.store_number
    
                """, connection)

            sales_report = sales_report.fillna(0)

            return sales_report

    def kroger_sales_by_period(self):

        '''
        takes all of the stores and finds the sales per period

        :return: df

        '''

        with DbConfig.EnginePoolDB() as connection:

            period_table =pd.read_sql(f"""
                with 

                    grocery_sales as (
                
                            select kroger_atlanta.sales2.*
                            from kroger_atlanta.sales2
                
                            union all
                
                
                            select kroger_central.sales2.*
                            from kroger_central.sales2
                
                            union all
                
                
                            select kroger_cincinatti.sales2.*
                            from kroger_cincinatti.sales2
                
                            union all
                
                
                            select kroger_columbus.sales2.*
                            from kroger_columbus.sales2
                
                            union all
                
                
                            select kroger_dallas.sales2.*
                            from kroger_dallas.sales2
                
                            union all
                
                
                            select kroger_delta.sales2.*
                            from kroger_delta.sales2
                
                            union all
                
                
                            select kroger_dillons.sales2.*
                            from kroger_dillons.sales2
                
                            union all
                
                
                            select kroger_king_soopers.sales2.*
                            from kroger_king_soopers.sales2
                
                            union all
                
                
                            select kroger_louisville.sales2.*
                            from kroger_louisville.sales2
                
                            union all
                
                
                            select kroger_michigan.sales2.*
                            from kroger_michigan.sales2
                
                            union all
                
                
                            select kroger_nashville.sales2.*
                            from kroger_nashville.sales2
                
                            )
                            
                    select kroger_period, round(sum(sales)) as sales, sum(qty) as qty_sold from grocery_sales
                    inner join kroger_periods on grocery_sales.store_week = kroger_periods.store_week
                    where store_year={self.store_year}
                    group by kroger_period
                    order by kroger_period
                
                """, connection)

        period_table = period_table.set_index('kroger_period')

        period_table.loc["Total"] = period_table.sum()

        return period_table

    def kroger_division_sales_by_period(self):

        '''

        :return: df object

        table that shows the sales $ and unit by period
        '''

        with DbConfig.EnginePoolDB() as connection:

            division_period_table = psql.read_sql(f'''
            
                with 
    
                    grocery_sales as (
                
                            select {self.store_type_input}.sales2.*
                            from {self.store_type_input}.sales2
                
                            )
                            
                select kroger_period, round(sum(sales)) as sales, sum(qty) as qty_sold from grocery_sales
                inner join kroger_periods on grocery_sales.store_week = kroger_periods.store_week
                where store_year={self.store_year}
                group by kroger_period
                order by kroger_period
            
            ''',connection)

        division_period_table = division_period_table.set_index('kroger_period')

        division_period_table.loc["Total"] = division_period_table.sum()

        return  division_period_table

    def kroger_corporate_sales_overview(self):

        with DbConfig.EnginePoolDB() as connection:

            overview = psql.read_sql(f'''
            
            with 

                grocery_sales as (
            
                        select kroger_atlanta.sales2.*
                        from kroger_atlanta.sales2
            
                        union all
            
            
                        select kroger_central.sales2.*
                        from kroger_central.sales2
            
                        union all
            
            
                        select kroger_cincinatti.sales2.*
                        from kroger_cincinatti.sales2
            
                        union all
            
            
                        select kroger_columbus.sales2.*
                        from kroger_columbus.sales2
            
                        union all
            
            
                        select kroger_dallas.sales2.*
                        from kroger_dallas.sales2
            
                        union all
            
            
                        select kroger_delta.sales2.*
                        from kroger_delta.sales2
            
                        union all
            
            
                        select kroger_dillons.sales2.*
                        from kroger_dillons.sales2
            
                        union all
            
            
                        select kroger_king_soopers.sales2.*
                        from kroger_king_soopers.sales2
            
                        union all
            
            
                        select kroger_louisville.sales2.*
                        from kroger_louisville.sales2
            
                        union all
            
            
                        select kroger_michigan.sales2.*
                        from kroger_michigan.sales2
            
                        union all
            
            
                        select kroger_nashville.sales2.*
                        from kroger_nashville.sales2),
                        
                kroger_division as (select distinct(store_type) from grocery_sales),
                
                ytd_sales as (select store_type, round(sum(sales)) as ytd_sales
                              from grocery_sales
                              where store_year = {self.store_year}
                              group by store_type),
                
                current_week_sales as (select store_type, round(sum(sales)) as current_wk_sales
                              from grocery_sales
                              where store_year = {self.store_year} and store_week = {self.week_num}
                              group by store_type),
                              
                previous_week_sales as (select store_type, round(sum(sales)) as prev_wk_sales
                          from grocery_sales
                          where store_year = {self.store_year} and store_week = {self.week_num}-1
                          group by store_type),
                
                /*stores are only considered active whenever a store has a sale on the most present year*/
                
                active_programs as (select store_type, sum(count) as active_stores
                                  from 
                                          (select count(distinct(store_number)), store_number, store_type
                                           from grocery_sales
                                           where store_year = {self.store_year}
                                           group by store_type, store_number) as active_stores
                                  
                                  group by store_type)
            
            select kroger_division.store_type, ytd_sales, current_wk_sales,  prev_wk_sales, active_stores
            
            from kroger_division
            
            inner join ytd_sales on ytd_sales.store_type = kroger_division.store_type
            
            inner join current_week_sales on current_week_sales.store_type = kroger_division.store_type
            
            inner join previous_week_sales on  previous_week_sales.store_type = kroger_division.store_type
            
            inner join active_programs on active_programs.store_type = kroger_division.store_type
            
            order by kroger_division.store_type

            ''', connection)

        overview = overview.set_index('store_type')

        overview.loc["Total"] = overview.sum()

        return overview






# store_type_input = 'kroger_cincinatti'
#
# store_setting = pd.read_excel(
#     rf'C:\Users\User1\OneDrive - winwinproducts.com\Groccery Store Program\{store_type_input}\{store_type_input}_store_setting.xlsm',
#     sheet_name='Sheet2',
#     header=None,
#     index_col=0,
#     names=('setting', 'values'))
# #
# test = ReportsData(store_type_input, store_setting)
# a = test.on_hands()
# # b = test.item_sales_rank_qty()
# #
# a.to_excel('sales-qty.xlsx')
# # b.to_excel('item-qty.xlsx')
#
#
# # ghost=test.ghost_inventory()
# # ghost.to_excel('dallas_ghost.xlsx')
