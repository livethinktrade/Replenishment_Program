# STEP 4 SALES DATA IMPORT
import psycopg2
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import pool
import numpy as np

from psycopg2.extensions import register_adapter, AsIs
from data_insertion import sales_insert
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


###fucntion takes in sales dataframe and connection to db for attributes
def salesimport(new_sales, connection_pool):


    new_len = len(new_sales)
    i=0
    while i < new_len:
            
        transition_date_range= new_sales.iloc[i,0]
        store_year= new_sales.iloc[i,1]
        store_week= new_sales.iloc[i,2]
        store_number= new_sales.iloc[i,3]
        upc= new_sales.iloc[i,4]
        sales= new_sales.iloc[i,5]
        qty= new_sales.iloc[i,6]
        current_year= new_sales.iloc[i,7]
        current_week= new_sales.iloc[i,8]
        store_type= new_sales.iloc[i,9]
        
        sales_insert(transition_date_range,store_year,store_week,store_number,upc,sales,qty,current_year,current_week,store_type,connection_pool)
        i +=1