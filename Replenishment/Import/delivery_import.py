# STEP 3 Delivery Data Import
import psycopg2
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import pool
import numpy as np

from psycopg2.extensions import register_adapter, AsIs
from data_insertion import delivery_insert
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def deliveryimport(new_deliv, connection_pool):
    
    new_len = len(new_deliv)
    i=0
    while i < new_len:
        transition_date_range = new_deliv.iloc[i,0]
        ttype = new_deliv.iloc[i,1]
        date = new_deliv.iloc[i,2]
        upc = new_deliv.iloc[i,3]
        store = new_deliv.iloc[i,4]
        qty = new_deliv.iloc[i,5]
        store_type = new_deliv.iloc[i,6]
        num = new_deliv.iloc[i,7]
        
        delivery_insert(transition_date_range,
                        ttype, 
                        date, 
                        upc, 
                        store, 
                        qty, 
                        store_type, 
                        num, 
                        connection_pool)


        i +=1

     