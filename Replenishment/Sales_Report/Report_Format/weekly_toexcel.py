from fileinput import filename
import psycopg2
import pandas as pd
import pandas.io.sql as psql

import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

import datetime


def weekly_toexcel(store_type_input, replenishment_report, sales_report, no_scan, sales_sql_YTD_Mask,
                   sales_sql_YTD_WoMask, item_sales_rank, on_hand, store_setting):
    # STEP 7 SALES REPORT TO EXCEL


    # connection = psycopg2.connect(database=f"{store_type_input}", user="postgres", password="winwin", host="localhost")

    date = datetime.date.today()
    date = date.strftime("%b-%d-%Y")
    filename = f'{store_type_input}_sales_report_{date}.xlsx'


    with pd.ExcelWriter(filename) as writer:
        replenishment_report.to_excel(writer, sheet_name="Replenishment",
                            index= False,
                            columns=('store_name', 'item','case','notes','case_qty','display_size'))


        sales_report.to_excel(writer, 
                        sheet_name="Sales Report", 
                        index= False, 
                        header=('Store (#)','Current Week Sales', 'Previous Week Sales','% +/- Change WOW', 'Sales ($) 2022 Current Week','Sales ($) 2021 Current Week','% +/- Change YOY','Sales ($) 2022 YTD','Sales ($) 2021 YTD','% +/- Change (YOY)')
                            )

        in_season_setting = store_setting.loc['In_Season', 'values']

        if in_season_setting == 1:

            header = ('store', 'item_group_desc', 'last shipped', 'weeks age')

        else:

            header = ('store', 'item')

        no_scan.to_excel(writer, sheet_name="No Scan", index= False, header= header)

        sales_sql_YTD_WoMask.to_excel(writer, sheet_name="Sales Report", 
                                index= False, 
                                columns=('ytd_current','ytd_previous','yoy_change'), 
                                header= ('Division Sales current YTD($) -Mask', 'Division Sales previous YTD($) -Mask', '% +/- YOY Change'), 
                                startrow= 5, 
                                startcol= 12)

        sales_sql_YTD_WoMask.to_excel(writer, sheet_name="Sales Report", 
                                index= False, 
                                columns=('store_supported','avg_wk_store','avg_month_store'), 
                                header= ('(#) Stores Supported', 'Avg Sales Per Wk/Store ($)', 'Avg Sales Per Mo/Store ($)'), 
                                startrow= 0, 
                                startcol= 12) 
        
        
        if store_type_input in ['fresh_encounter', 'jewel', 'texas_division']:
            

            sales_sql_YTD_Mask.to_excel(writer, sheet_name="Sales Report", 
                                    index= False, 
                                    columns=('ytd_current','ytd_previous','yoy_change'), 
                                    header= ('Division Sales current YTD($) +Mask', 'Division Sales previous YTD($) +Mask', '% +/- YOY Change'), 
                                    startrow= 10, 
                                    startcol= 12)

        item_sales_rank.to_excel(writer, sheet_name="Sales Report", 
                                index= True, 
                                columns=('item_group_desc', 'sales'), 
                                header= ('item','sales ($)'), 
                                startrow= 14, 
                                startcol= 12)    
        
        
        on_hand.to_excel(writer, sheet_name="On Hand", index= False)

        return filename



