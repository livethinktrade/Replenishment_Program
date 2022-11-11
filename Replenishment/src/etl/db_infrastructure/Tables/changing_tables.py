# this script is used in the event you need to start changing up the table structures for all of the stores.
# Built this so you don't have to change each one of them manually
from config.DbConfig import *

from src.store_info import Replenishment
#

def size_table_insert(store_type_input, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""

           Create Table {store_type_input}.year_week_verify (
            store_year integer,
            store_week integer,
            store_type character varying(20)
)
            
""")

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)

store_list = [

    'acme',
    'follett',
    'fresh_encounter',
    'intermountain',
    'jewel',
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
    'kroger_nashville',
    'kvat',
    'safeway_denver',
    'texas_division'
]

with PsycoPoolDB() as connection:

    for x in store_list:

        store = Replenishment(store_type_input=f'{x}')

        try:
            size_table_insert(store.store_type_input, connection)

        except Exception as e:
            print(e)
            print(f'for {x}')