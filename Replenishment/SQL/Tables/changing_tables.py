# this script is used in the event you need to start changing up the table structures for all of the stores.
# Built this so you don't have to change each one of them manually


from store_info import Replenishment
#




def size_table_insert(store_type_input, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""

            ALTER TABLE {store_type_input}.store_program_history drop constraint store_program_history_pkey;
            ALTER TABLE {store_type_input}.store_program_history ADD PRIMARY KEY (history_id)
            
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
    # 'kroger_central',
    'kroger_cincinatti',
    'kroger_columbus',
    'kroger_dallas',
    # 'kroger_delta',
    'kroger_dillons',
    'kroger_king_soopers',
    'kroger_louisville',
    'kroger_michigan',
    'kroger_nashville',
    # 'kvat',
    'safeway_denver',
    'texas_division'
]


for x in store_list:

    store = Replenishment(store_type_input=f'{x}')

    try:
        size_table_insert(store.store_type_input, store.connection_pool)

    except Exception as e:
        print(e)
        print(f'for {x}')