from fileinput import filename
import psycopg2
import pandas as pd
import pandas.io.sql as psql

import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

import datetime

def ytd_toexcel(store_type_input, replenishment_report, sales_report, no_scan, sales_sql_YTD_WoMask, item_sales_rank, on_hand):
    
    connection = psycopg2.connect(database=f"{store_type_input}", user="postgres", password="winwin", host="localhost")

    sales_report = sales_report.drop(columns=['current_week',
                                            'previous_week', 
                                            'wow_sales_percentage',
                                            'current_week',
                                            'previous_year_week',
                                            'yoy_sales_percentage',
                                            'ytd_2021',
                                            'yoy_sales_percentage'])
    sales_report = sales_report.dropna()
    sales_report = sales_report.head(20)

    date = datetime.date.today()
    date = date.strftime("%b-%d-%Y")
    filename = f'{store_type_input}_sales_report_{date}.xlsx'


    with pd.ExcelWriter(filename) as writer:
        replenishment_report.to_excel(writer, sheet_name="Replenishment",
                            index= False,
                            columns=('store', 'item','case','notes','case_qty','display_size'))


        sales_report.to_excel(writer, 
                        sheet_name="Sales Report", 
                        index= False, 
                        header=('Store (#)','Sales ($) 2022 YTD'),
                        startrow = 1,
                        startcol = 0)
        

    
        no_scan.to_excel(writer, sheet_name="No Scan", index= False, header= ('store', 'item')) 

    

        sales_sql_YTD_WoMask.to_excel(writer, sheet_name="Sales Report", 
                                index= False, 
                                columns=('ytd_current','ytd_previous','yoy_change'), 
                                header= ('Division Sales current YTD($) -Mask', 'Division Sales previous YTD($) -Mask', '% +/- YOY Change'), 
                                startrow= 5, 
                                startcol= 4)

        sales_sql_YTD_WoMask.to_excel(writer, sheet_name="Sales Report", 
                                index= False, 
                                columns=('store_supported','avg_wk_store','avg_month_store'), 
                                header= ('(#) Stores Supported', 'Avg Sales Per Wk/Store ($)', 'Avg Sales Per Mo/Store ($)'), 
                                startrow= 0, 
                                startcol= 4) 

        # sales_sql_YTD_Mask.to_excel(writer, sheet_name="Sales Report", 
        #                         index= False, 
        #                         columns=('ytd_current','ytd_previous','yoy_change'), 
        #                         header= ('Division Sales current YTD($) +Mask', 'Division Sales previous YTD($) +Mask', '% +/- YOY Change'), 
        #                         startrow= 10, 
        #                         startcol= 12)

        item_sales_rank.to_excel(writer, sheet_name="Sales Report", 
                                index= True, 
                                columns=('item_group_desc', 'sales'), 
                                header= ('item','sales ($)'), 
                                startrow= 10, 
                                startcol= 4)    
        
        
        on_hand.to_excel(writer, sheet_name="On Hand", index= False)

        return filename
