import pandas as pd
import psycopg2
import numpy as np
import DbConfig
from psycopg2.extensions import register_adapter, AsIs

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

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


quickbooks_store_list = {

            'ACME MARKETS': 'acme',
            'JEWEL OSCO': 'jewel',
            'KROGER CENTRAL': 'kroger_central',
            'INTERMOUNTAIN DIVISION': 'intermountain',
            'KROGER COLUMBUS': 'kroger_columbus',
            'KROGER DALLAS': 'kroger_dallas',
            'KROGER DELTA': 'kroger_delta',
            'KROGER MICHIGAN': 'kroger_michigan',
            'ALBERTSONS DENVER': 'safeway_denver',
            'TEXAS DIVISION': 'texas_division',
            'KVAT FOOD STORES': 'kvat',
            'FRESH ENCOUNTER': 'fresh_encounter',
            'KROGER KING SOOPERS': 'kroger_king_soopers',
            'KROGER DILLONS': 'kroger_dillons',
            'KROGER CINCINNATI': 'kroger_cincinatti',
            'KROGER ATLANTA': 'kroger_atlanta',
            'KROGER NASHVILLE': 'kroger_nashville',
            'KROGER LOUISVILLE': 'kroger_louisville',
            'FOLLETT': 'follett'

        }