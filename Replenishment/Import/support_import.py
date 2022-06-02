
import psycopg2
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import pool
import numpy as np

from psycopg2.extensions import register_adapter, AsIs
from data_insertion import item_support_insert
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

def supportimport(itemsupport,connection_pool):

    item_support = len(itemsupport)
    i=0
    while i < item_support:
        
        upc =itemsupport.iloc[i,0]
        upc_11_digit=itemsupport.iloc[i,1]
        season=itemsupport.iloc[i,2]
        type=itemsupport.iloc[i,3]
        style=itemsupport.iloc[i,4]
        gm=itemsupport.iloc[i,5]
        additional=itemsupport.iloc[i,6]
        code_qb=itemsupport.iloc[i,7]
        item_desc=itemsupport.iloc[i,8]
        unit=itemsupport.iloc[i,9]
        salesrank=itemsupport.iloc[i,10]
        total_case_size=itemsupport.iloc[i,11]
        shipped_per_case=itemsupport.iloc[i,12]
        mupc=itemsupport.iloc[i,13]
        item_group_desc=itemsupport.iloc[i,14]
        display_size=itemsupport.iloc[i,15]
        size=itemsupport.iloc[i,16]
        availability = itemsupport.iloc[i,17]
        
        item_support_insert(upc= upc, 
                        upc_11_digit = upc_11_digit, 
                        season= season, 
                        type= type, 
                        style= style, 
                        gm= gm, 
                        additional= additional, 
                        code_qb= code_qb, 
                        item_desc= item_desc, 
                        unit= unit, 
                        salesrank= salesrank, 
                        total_case_size= total_case_size, 
                        shipped_per_case= shipped_per_case, 
                        mupc= mupc, 
                        item_group_desc= item_group_desc,
                        display_size= display_size,
                        size= size,
                        availability= availability, 
                        connection_pool = connection_pool)
        i +=1 