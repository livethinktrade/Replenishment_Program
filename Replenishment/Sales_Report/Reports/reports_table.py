import psycopg2
import pandas as pd
import pandas.io.sql as psql

import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def reports_table(store_type_input):
    # STEP 6 SALES REPORT 


    # if store_type_input in ['kroger_dallas', 'kroger_central', 'kroger_delta', 'kroger_michigan', 'kroger_columbus']:
    #     week = 'store_week'
    # else:
    #     week = 'current_week'
    #
    # #Gets the replenishment recomendations
    # connection = psycopg2.connect(database=f"{store_type_input}", user="postgres", password="winwin", host="localhost")
    #
    # #finds the current year per winwin company year
    # year = psql.read_sql("select max(current_year) from sales;", connection)
    #
    # #finds the current week the store is on
    # store_year = year.iloc[0,0]
    # num = psql.read_sql(f"select max({week}) from sales where store_year = {store_year}", connection)
    # week_num = num.iloc[0,0]
    #
    # #This line is neccesary for jewel because when the sales data comes in on monday it usually
    # # includes sales that were made the previous day on Sunday. This is a problem because Sunday marks the beginning of the new week
    # #which meesses up the weekly reporting
    #
    # if store_type_input == 'jewel':
    #     week_num -= 1
    #
    # #finds the number of weeks the store has been active in the current year. more important for new stores rather than old.
    # # helps calculate the avg store sales
    # num = psql.read_sql(f"select distinct({week}) from sales where store_year = {store_year}", connection)
    # num = len(num)
    #
    #
    # #creates sales table along with sales $ per store and previous year performance
    #
    # sales_sql = f"""
    # with date as (
    #     select distinct store_number
    #     from sales
    #     order by store_number asc),
    #
    #     current_week as (
    #
    #         select store_number, store_year, {week}, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #
    #         where store_year = {store_year} and
    #             {week} = {week_num} and
    #             (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW')
    #
    #         group by store_number, store_year, {week}
    #
    #         order by store_number asc),
    #
    #     previous_week as (
    #
    #         select store_number, store_year, {week}, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #
    #         where store_year = {store_year} and
    #             {week} = {week_num}-1 and
    #             (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW')
    #
    #         group by store_number, store_year, {week}
    #
    #         order by store_number asc),
    #
    #     previous_year_week as (
    #         select store_number, store_year, {week}, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #
    #         where store_year = {store_year}-1 and
    #             {week} = {week_num} and
    #             (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW')
    #
    #         group by store_number, store_year, {week}
    #
    #         order by store_number asc),
    #
    #     current_ytd_week as (
    #
    #         select store_number, store_year, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #
    #         where store_year = {store_year} and
    #             {week} <= {week_num} and
    #             (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW')
    #
    #         group by store_number, store_year
    #
    #         order by store_number asc),
    #
    #
    #     previous_ytd_week as (
    #
    #
    #         select store_number, store_year, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #         /*looks for previous year so maybe max -1 when finding innitial start for python program*/
    #         where store_year = {store_year}-1 and
    #             {week} <= {week_num} and
    #             (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW')
    #
    #         group by store_number, store_year
    #
    #         order by store_number asc)
    #
    # select
    #     date.store_number,
    #     current_week.sales as current_week,
    #     previous_week.sales as previous_week,
    #     /*WOW sales % */
    #     case
    #         when current_week.sales < 0 or previous_week.sales <= 0
    #             then NULL
    #         when current_week.sales >= 0 or previous_week.sales > 0
    #             then round(((current_week.sales-previous_week.sales)/(previous_week.sales)),2)
    #         end as WOW_sales_percentage,
    #
    #
    #
    #     current_week.sales as current_week,
    #     previous_year_week.sales as previous_year_week,
    #     case
    #         when current_week.sales < 0 or previous_year_week.sales <= 0
    #             then NULL
    #         when current_week.sales >= 0 or previous_year_week.sales > 0
    #             then round(((current_week.sales-previous_year_week.sales)/(previous_year_week.sales)),2)
    #         end as YoY_sales_percentage,
    #
    #
    #     current_ytd_week.sales as YTD_2022,
    #     previous_ytd_week.sales as YTD_2021,
    #     case
    #         when current_ytd_week.sales < 0 or previous_ytd_week.sales <= 0
    #             then NULL
    #         when current_ytd_week.sales >= 0 or previous_ytd_week.sales > 0
    #             then round(((current_ytd_week.sales-previous_ytd_week.sales)/(previous_ytd_week.sales)),2)
    #         end as YoY_sales_percentage
    #
    #
    # from date
    # full join current_week on date.store_number = current_week.store_number
    # full join previous_week on date.store_number = previous_week.store_number
    # full join previous_year_week on date. store_number = previous_year_week.store_number
    # full join current_ytd_week on date.store_number = current_ytd_week.store_number
    # full join previous_ytd_week on date.store_number = previous_ytd_week.store_number
    #
    # order by current_ytd_week.sales desc
    #
    # """


    # #This will generate YTD Sales and preveious YTD sales "Without Mask"
    # sales_sql_YTD_WoMask = f"""
    #
    # with
    #     store_count as (
    #         select distinct store_number
    #         from sales
    #         where store_year = {store_year}),
    #
    #
    #     current_ytd_week as (
    #
    #         select store_number, store_year, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #
    #         where store_year = {store_year} and
    #             {week} <= {week_num} and
    #             (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW')
    #
    #         group by store_number, store_year
    #
    #         order by store_number asc),
    #
    #     previous_ytd_week as (
    #
    #
    #         select store_number, store_year, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #
    #         /*looks for previous year so maybe max -1 when finding innitial start for python program*/
    #         where store_year = {store_year}-1 and
    #             {week} <= {week_num} and
    #             (item_support.gm = 'AY' or item_support.gm = 'SS' or item_support.gm = 'FW')
    #
    #         group by store_number, store_year
    #
    #         order by store_number asc),
    #
    #     yearly_sum as (
    #
    #         select round(sum(sales)) as YTD_current,
    #             (select round(sum(sales)) as YTD_previous from previous_ytd_week)
    #         from current_ytd_week),
    #
    #     yearly_sum_p2 as (
    #         select ytd_current, ytd_previous,
    #                 ((ytd_current-ytd_previous)/ytd_previous) as YoY_Change,
    #             (select count(store_number) from store_count) as store_supported
    #         from yearly_sum)
    #
    # select
    #     ytd_current,
    #     ytd_previous,
    #     yoy_change,
    #
    #     store_supported,
    #     /*change number to current week of store year to make program dynamic
    #     next line calculates Avg Sales Per Wk/Store ($)*/
    #     round(ytd_current/store_supported/{num}) as avg_wk_store,
    #
    #     /*next line calculates Avg Sales Per month/Store ($)*/
    #     round((ytd_current/store_supported/{num})*4) as avg_month_store
    #
    # from yearly_sum_p2
    # """

    #this will generate YTD sales and previous YTD "WITH MASK"
    # sales_sql_YTD_Mask = f"""
    #
    # with
    #     store_count as (
    #         select distinct store_number
    #         from sales
    #         where store_year = {store_year}),
    #
    #
    #     current_ytd_week as (
    #
    #         select store_number, store_year, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #
    #         where store_year = {store_year} and
    #             {week} <= {week_num}
    #
    #         group by store_number, store_year
    #
    #         order by store_number asc),
    #
    #     previous_ytd_week as (
    #
    #
    #         select store_number, store_year, sum(sales) as sales
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #
    #         /*looks for previous year so maybe max -1 when finding innitial start for python program*/
    #         where store_year = {store_year}-1 and
    #             {week} <= {week_num}
    #
    #         group by store_number, store_year
    #
    #         order by store_number asc),
    #
    #     yearly_sum as (
    #
    #         select round(sum(sales)) as YTD_current,
    #             (select round(sum(sales)) as YTD_previous from previous_ytd_week)
    #         from current_ytd_week),
    #
    #     yearly_sum_p2 as (
    #         select ytd_current, ytd_previous,
    #                 ((ytd_current-ytd_previous)/ytd_previous) as YoY_Change,
    #             (select count(store_number) from store_count) as store_supported
    #         from yearly_sum)
    #
    # select
    #     ytd_current,
    #     ytd_previous,
    #     yoy_change,
    #
    #     store_supported,
    #     /*change number to current week of store year to make program dynamic
    #     next line calculates Avg Sales Per Wk/Store ($)*/
    #     round(ytd_current/store_supported/{num}) as avg_wk_store,
    #
    #     /*next line calculates Avg Sales Per month/Store ($)*/
    #     round((ytd_current/store_supported/{num})*4) as avg_month_store
    #
    # from yearly_sum_p2
    # """
    # item_sales_rank = f'''
    # with
    #     year_total as(
    #         Select sum(sales) as year_sales, sum(qty) as year_total_unit
    #         from sales
    #         where store_year = {store_year}
    #     ),
    #
    #     item_sales_rank as (
    #         Select item_group_desc, round(sum(sales),2) as sales, sum(qty) as units_sold,season
    #         from sales
    #         inner join item_support on sales.upc = item_support.upc_11_digit
    #         where store_year = {store_year}
    #         group by item_group_desc, season
    #         order by sum(sales) desc)
    #
    #
    # select item_group_desc,
    #     sales,
    #     units_sold,
    #     round((sales/year_sales),3) as percent_of_total_sales,
    #     season
    # from item_sales_rank, year_total
    # '''
    #
    # item_sales_rank = psql.read_sql(f'{item_sales_rank}', connection)
    # item_sales_rank= item_sales_rank.head(10)
    #
    #
    # sales_sql_YTD_WoMask = psql.read_sql(f'{sales_sql_YTD_WoMask}', connection)
    # sales_sql_YTD_Mask = psql.read_sql(f'{sales_sql_YTD_Mask}', connection)
    #
    #
    #
    #
    # sales_report = psql.read_sql(f'{sales_sql}', connection)
    # sales_report_len = len(sales_report)+1

#on_hand report
    on_hand = psql.read_sql("SELECT * FROM SD_COMBO WHERE season = 'AY' or season = 'SS' order by store asc", connection)


#no scan below is only for jewel osco to see if tops have been scanned in
#no scan code
    if store_type_input in ['jewel']:
        no_scan = psql.read_sql("""
        
        with long_hanging as (
        select * from sd_combo 
        where (display_size = 'Long Hanging Top' or display_size = 'Long Hanging Dress') and
	            season = 'SS'),

        total as (
        select store, sum(total_sales)as sales from long_hanging
        group by store)

        select distinct store, sales from total
        where sales = 0
        

        """, connection)

    elif store_type_input in ['kroger_columbus']:

        no_scan = psql.read_sql("""
        
        select distinct store, sum(total_sales) as total_sales
        from sd_combo
        group by store
        having sum(total_sales) = 0

        """, connection)

    else:

        max_date = psql.read_sql('select max(date) from delivery', connection)
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

        """, connection)




    return sales_report, item_sales_rank, sales_sql_YTD_Mask, sales_sql_YTD_WoMask, sales_report_len, on_hand, no_scan

