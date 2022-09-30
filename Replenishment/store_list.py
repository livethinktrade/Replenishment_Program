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
