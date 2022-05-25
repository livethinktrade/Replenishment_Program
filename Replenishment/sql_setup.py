
import psycopg2

def sql_setup(sql_code):
    store_list = [  'acme','fresh_encounter', 'intermountain', 
                    'jewel','kroger_central', 'kroger_columbus',
                    'kroger_dallas', 'kroger_delta', 'kroger_michigan',
                    'kvat', 'safeway_denver', 'texas_division']

    i= 0
    num_of_store = len(store_list)

    while i < num_of_store:

        store = store_list[i]
        connection = psycopg2.connect(database=f"{store}", user="postgres", password="winwin", host="localhost")

        cursor = connection.cursor()
        cursor.execute(f''' {sql_code}
        ''')
        
        connection.commit()
        cursor.close()

        i+=1
