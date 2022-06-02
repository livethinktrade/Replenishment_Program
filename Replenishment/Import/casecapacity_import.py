import psycopg2
import pandas as pd
import pandas.io.sql as psql
from psycopg2 import pool
import numpy as np

from psycopg2.extensions import register_adapter, AsIs
from data_insertion import casecapacity_insert
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def casecapacity_import(case_capacity, connection_pool):

    new_len = len(case_capacity)
    i=0
    while i < new_len:
        store_type = case_capacity.iloc[i,0]
        store= case_capacity.iloc[i,1]
        rack_4w= case_capacity.iloc[i,2]
        rack_1w =case_capacity.iloc[i,3]
        rack_2w= case_capacity.iloc[i,4]
        rack_pw= case_capacity.iloc[i,5]
        carded= case_capacity.iloc[i,6]
        long_hanging_top = case_capacity.iloc[i,7]
        long_hanging_dress=case_capacity.iloc[i,8]
        case_cap= case_capacity.iloc[i,9]
        notes= case_capacity.iloc[i,10]
        initial = case_capacity.iloc[i,11]

        casecapacity_insert(store_type,store,rack_4w,rack_1w,rack_2w,rack_pw,carded,long_hanging_top,long_hanging_dress,case_cap,notes,initial,connection_pool)
        
        i+=1