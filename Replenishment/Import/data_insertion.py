# STEP 2 IMPORT DATA FUNCTIONS

import psycopg2
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import pool
import numpy as np

from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def casecapacity_insert(store_type,
                    store,
                    rack_4w,
                    rack_1w,
                    rack_2w,
                    rack_pw,
                    carded,
                    long_hanging_top,
                    long_hanging_dress,
                    case_capacity,notes,
                    initial,
                    connection_pool):
 
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO case_capacity (store_type, store, rack_4w, rack_1w, rack_2w,rack_pw,carded,long_hanging_top,long_hanging_dress,case_capacity,notes, initial) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
           (store_type,
           store,
           rack_4w,
           rack_1w,
           rack_2w,
           rack_pw,
           carded,
           long_hanging_top,
           long_hanging_dress,
           case_capacity,
           notes,
           initial))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)



# this is for updating prexisting data in the DB
def deliveryupdate(transition_date_range, type, date, upc, store, qty, store_type,connection_pool):
 
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"""update delivery set qty = {qty}, transition_date_range ='{transition_date_range}' 
                        WHERE type ='{type}' and 
                        date = '{date}' and 
                        store = {store} and 
                        upc = '{upc}' and 
                        store_type = '{store_type}'
                    """)
    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)



def delivery_insert(transition_date_range,type, date , upc, store, qty, store_type, num, connection_pool):
 
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO delivery (transition_date_range,type, date, upc, store, qty, store_type, num) values (%s,%s,%s,%s,%s,%s,%s,%s)",
           (transition_date_range,type,date,upc,store,qty, store_type,num))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


# this is for updating prexisting data in the DB
def salesupdate(transition_date_range,	
                store_year,	
                store_week,	
                store_number,	
                upc,	
                sales,	
                qty,	
                current_year,	
                current_week,	
                store_type,
                connection_pool):
 
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"""update sales 
                        set qty = {qty}, 
                            transition_date_range ='{transition_date_range}',
                            sales={sales},
                            current_year = {current_year},
                            current_week = {current_week}
                        WHERE store_year ={store_year} and 
                        store_week = '{store_week}' and 
                        store_number = {store_number} and 
                        upc = '{upc}' and 
                        store_type = '{store_type}'
                    """)
    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


    
def sales_insert(transition_date_range,
                store_year,
                store_week,
                store_number,
                upc,
                sales,
                qty,
                current_year,
                current_week,
                store_type,
                connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO sales (transition_date_range,store_year,store_week,store_number,upc,sales,qty,current_year,current_week,store_type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                   (transition_date_range,store_year,store_week,store_number,upc,sales,qty,current_year,current_week,store_type))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)
    
def item_support_insert(upc, 
                      upc_11_digit, 
                      season, 
                      type, 
                      style, 
                      gm, 
                      additional, 
                      code_qb, 
                      item_desc, 
                      unit, 
                      salesrank, 
                      total_case_size, 
                      shipped_per_case, 
                      mupc, 
                      item_group_desc,
                      display_size,
                      size,
                      availability,
                      connection_pool):
    
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO item_support (upc, upc_11_digit, season,type,style,gm,additional,code_qb, item_desc, unit, salesrank, total_case_size, shipped_per_case, mupc, item_group_desc, display_size,size,availability) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                   (upc, 
                    upc_11_digit, 
                    season, 
                    type, 
                    style, 
                    gm, 
                    additional, 
                    code_qb, 
                    item_desc, 
                    unit, 
                    salesrank, 
                    total_case_size, 
                    shipped_per_case, 
                    mupc, 
                    item_group_desc,
                    display_size,
                    size,
                    availability))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)

