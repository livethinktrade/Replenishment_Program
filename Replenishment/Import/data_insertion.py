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

# new code for deliv insert
# def delivery_insert(transition_year,
#                     transition_season,
#                     type, date, upc, store, qty, store_type,
#                     num, code, connection_pool):
#
#     connection = connection_pool.getconn()
#     cursor = connection.cursor()
#     cursor.execute(
#         """INSERT INTO delivery2 (transition_year, transition_season,type, date, upc, store, qty, store_type, num, code)
#                                 values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
#         (transition_year, transition_season, type, date, upc, store, qty, store_type, num, code))
#
#     connection.commit()
#     cursor.close()
#     connection_pool.putconn(connection)


# old delivery_insert
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

# new code for sales insert
# def sales_insert(transition_year,
#                  transition_season,
#                  store_year,
#                  store_week,
#                  store_number,
#                  upc,
#                  sales,
#                  qty,
#                  current_year,
#                  current_week,
#                  store_type,
#                  connection_pool):
#
#     connection = connection_pool.getconn()
#     cursor = connection.cursor()
#     cursor.execute("""INSERT INTO sales2 (transition_year, transition_season,store_year,
#                                         store_week,store_number,upc,sales,qty,current_year,
#                                         current_week,store_type)
#                         values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
#
#                    (transition_year, transition_season, store_year, store_week,
#                     store_number, upc, sales, qty, current_year, current_week, store_type))
#
#     connection.commit()
#     cursor.close()
#     connection_pool.putconn(connection)


# old version sales insert
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



def item_support_insert(season,
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
                        connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO item_support2 (season, category, type, style, additional, display_size, pog_type, 
                                                 upc, code, code_qb, unique_replen_code, case_size, item_group_desc,
                                                item_desc, packing, upc_11_digit) 
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",

                    (season,
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
                      upc_11_digit)

                  )

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)

#old version support insert
# def item_support_insert(upc,
#                       upc_11_digit,
#                       season,
#                       type,
#                       style,
#                       gm,
#                       additional,
#                       code_qb,
#                       item_desc,
#                       unit,
#                       salesrank,
#                       total_case_size,
#                       shipped_per_case,
#                       mupc,
#                       item_group_desc,
#                       display_size,
#                       size,
#                       availability,
#                       connection_pool):
#
    # connection = connection_pool.getconn()
    # cursor = connection.cursor()
#     cursor.execute("INSERT INTO item_support (upc, upc_11_digit, season,type,style,gm,additional,code_qb, item_desc, unit, salesrank, total_case_size, shipped_per_case, mupc, item_group_desc, display_size,size,availability) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
#                    (upc,
#                     upc_11_digit,
#                     season,
#                     type,
#                     style,
#                     gm,
#                     additional,
#                     code_qb,
#                     item_desc,
#                     unit,
#                     salesrank,
#                     total_case_size,
#                     shipped_per_case,
#                     mupc,
#                     item_group_desc,
#                     display_size,
#                     size,
#                     availability))
# #
    # connection.commit()
    # cursor.close()
    # connection_pool.putconn(connection)


def store_insert(store_id, initial, notes, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO store (store_id, initial, notes) values (%s,%s,%s)",
           (store_id, initial, notes))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def store_program_insert(store_id, program_id, activity, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO store_program (store_id, program_id, activity) values (%s,%s,%s)",
           (store_id, program_id, activity))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def master_planogram_insert(program_id, carded, long_hanging_top, long_hanging_dress, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO master_planogram (program_id, carded, long_hanging_top, long_hanging_dress) 
                    values (%s,%s,%s,%s)""",
           (program_id, carded, long_hanging_top, long_hanging_dress))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def item_approval_insert(code, store_price, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO item_approval (code, store_price) values (%s,%s)",
                   (code, store_price))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def inventory_insert(code, on_hand, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO inventory (code, on_hand) values (%s,%s)",
                   (code, on_hand))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)
