# this script is used in the event you need to start changing up the table structures for all of the stores.
# Built this so you don't have to change each one of them manually
from config.DbConfig import *

from src.store_info import DbUpdater
#

def size_table_insert(store_type_input, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""

    Create Table {store_type_input}.bandaids (
        type character varying(11) NOT NULL,
        store_id integer not null,
        item_group_desc varchar(50),
        qty numeric not null,
        date_created date NOT NULL,
        effective_date date NOT NULL,
        store_type character varying(20) NOT NULL,
        reason character varying(100) NOT NULL,
    
        PRIMARY KEY(store_id, item_group_desc, effective_date)
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
    'texas_division',
    'midwest',
    'sal'
]

with PsycoPoolDB() as connection:

    for x in store_list:

        store = DbUpdater(store_type_input=f'{x}')

        try:
            size_table_insert(store.store_type_input, connection)

        except Exception as e:
            print(e)
            print(f'for {x}')