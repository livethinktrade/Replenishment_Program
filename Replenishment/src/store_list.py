import pandas as pd
import psycopg2
import numpy as np
from psycopg2.extensions import register_adapter

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

store_list = pd.read_excel(r'C:\Users\User1\OneDrive\WinWin Staff Folders\Michael\Replenishment program\Replenishment\support document\Store List.xlsx')

kroger_stores = [

    'kroger_atlanta',
    'kroger_central',
    'kroger_cincinatti',
    'kroger_columbus',
    'kroger_dallas',
    'kroger_delta',
    'kroger_dillons',
    'kroger_king_soopers',
    'kroger_louisville',
    'kroger_michigan',
    'kroger_nashville'

]

albertson_division = ['texas_division', 'intermountain', 'acme']


fresh_encounter = ['sal', 'midwest']
