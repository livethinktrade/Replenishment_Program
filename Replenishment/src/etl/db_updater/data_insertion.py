# STEP 2 IMPORT DATA FUNCTIONS

import psycopg2
import numpy as np
from psycopg2 import extras
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


def casecapacity_insert(store_type,
                        store,
                        rack_4w,
                        rack_1w,
                        rack_2w,
                        rack_pw,
                        carded,
                        long_hanging_top,
                        long_hanging_dress,
                        case_capacity,notes,
                        initial,
                        connection_pool):
 
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("""
    INSERT INTO case_capacity (store_type, store, rack_4w, rack_1w, rack_2w,rack_pw,carded,
                               long_hanging_top,long_hanging_dress,case_capacity,notes, initial) 
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
            store_type, store, rack_4w,
            rack_1w, rack_2w, rack_pw,
            carded, long_hanging_top, long_hanging_dress,
            case_capacity, notes, initial))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def delivery_update(transition_year, transition_season,
                   type, date, upc, store, qty,
                   store_type, num, code, connection_pool, store_type_input):

    """ new delivery update"""

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"""update {store_type_input}.delivery2 set qty = {qty}, transition_year ='{transition_year}', 
                        transition_season ='{transition_season}' 
                        WHERE type ='{type}' and 
                        date = '{date}' and 
                        store = {store} and 
                        upc = '{upc}' and 
                        store_type = '{store_type}' and
                        code = '{code}' and
                        num = '{num}'
                    """)
    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def delivery_insert(transition_year,
                    transition_season,
                    type, date, upc, store, qty, store_type,
                    num, code, connection_pool, store_type_input):

    """new delivery insert"""

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""INSERT INTO {store_type_input}.delivery2 (transition_year, transition_season,type, date, upc, store, 
                                                      qty, store_type, num, code)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (transition_year, transition_season, type, date, upc, store, qty, store_type, num, code))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def bandaid_insert(type, store_id, item_group_desc, qty,
                   date_created, effective_date, store_type, reason,
                   connection_pool, store_type_input):

    """new delivery insert"""

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""INSERT INTO {store_type_input}.bandaids (type, store_id, item_group_desc, qty, 
                                                     date_created, effective_date, store_type, reason)
            values (%s,%s,%s,%s,%s,%s,%s,%s)""",
        (type, store_id, item_group_desc, qty, date_created, effective_date, store_type, reason))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


# this is for updating prexisting data in the DB
def salesupdate(transition_year,
                transition_season,
                store_year,
                date,
                store_week,	
                store_number,	
                upc,	
                sales,	
                qty,	
                current_year,	
                current_week,
                code,
                store_type,
                connection_pool,
                store_type_input):
 
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"""update {store_type_input}.sales2 
                        set qty = {qty}, 
                            transition_year ='{transition_year}',
                            transition_season = '{transition_season}',
                            sales={sales},
                            current_year = {current_year},
                            current_week = {current_week},
                            code = '{code}'
                        WHERE store_year ={store_year} and 
                        store_week = '{store_week}' and 
                        store_number = {store_number} and 
                        upc = '{upc}' and 
                        store_type = '{store_type}' and
                        date = '{date}'
                    """)
    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def sales_insert(transition_year,
                 transition_season,
                 store_year,
                 date,
                 store_week,
                 store_number,
                 upc,
                 sales,
                 qty,
                 current_year,
                 current_week,
                 code,
                 store_type,
                 connection_pool,
                 store_type_input):

    """New Sales Insert"""

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"""INSERT INTO {store_type_input}.sales2 (transition_year, transition_season,store_year, date,
                                        store_week,store_number,upc,sales,qty,current_year,
                                        current_week,code,store_type)
                        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",

                   (transition_year, transition_season, store_year, date, store_week,
                    store_number, upc, sales, qty, current_year, current_week, code, store_type))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)

def item_support_insert(season,
                        category,
                        type,
                        style,
                        additional,
                        display_size,
                        pog_type,
                        upc,
                        code,
                        code_qb,
                        unique_replen_code,
                        case_size,
                        item_group_desc,
                        item_desc,
                        packing,
                        upc_11_digit,
                        connection_pool):

    """ new item support sheet insert"""

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO item_support2 (season, category, type, style, additional, display_size, pog_type, 
                                                 upc, code, code_qb, unique_replen_code, case_size, item_group_desc,
                                                item_desc, packing, upc_11_digit) 
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                    season, category, type, style,
                    additional, display_size, pog_type,
                    upc, code, code_qb, unique_replen_code,
                    case_size, item_group_desc,
                    item_desc, packing, upc_11_digit)

                  )

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)

def item_support_update(season,
                        category,
                        type,
                        style,
                        additional,
                        display_size,
                        pog_type,
                        upc,
                        code,
                        code_qb,
                        unique_replen_code,
                        case_size,
                        item_group_desc,
                        item_desc,
                        packing,
                        upc_11_digit,
                        connection_pool):

    """ new item support sheet insert"""

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"""Update item_support2 set season = '{season}', 
                                                category = '{category}', 
                                                type = '{type}', 
                                                style = '{style}', 
                                                additional = '{additional}', 
                                                display_size = '{display_size}', 
                                                pog_type = '{pog_type}', 
                                                upc = '{upc}', 
                                                code_qb = '{code_qb}', 
                                                unique_replen_code = '{unique_replen_code}', 
                                                case_size = {case_size}, 
                                                item_group_desc = '{item_group_desc}',
                                                item_desc = '{item_desc}', 
                                                packing = {packing}, 
                                                upc_11_digit = '{upc_11_digit}'
                                            where code = '{code}' """)

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def store_insert(store_id, initial, notes, store_type, connection_pool, store_type_input):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"INSERT INTO {store_type_input}.store (store_id, initial, notes, store_type) values (%s,%s,%s,%s)",
           (store_id, initial, notes, store_type))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def store_update(store_id, initial, notes, store_type, connection_pool, store_type_input):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""update {store_type_input}.store set initial = '{initial}',
                                     notes = '{notes}',
                                     store_type = '{store_type}'
             where store_id = '{store_id}'""")

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def store_program_insert(store_program_id,store_id, program_id, activity, store_type, connection_pool, store_type_input):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"""
    
    INSERT INTO {store_type_input}.store_program (store_program_id, store_id, program_id, activity,store_type) 
    values (%s,%s,%s,%s,%s)""", (store_program_id, store_id, program_id, activity, store_type))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def store_program_update(store_program_id,store_id, program_id, activity, store_type, connection_pool, store_type_input):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""update {store_type_input}.store_program set store_id = '{store_id}',
                                    program_id = '{program_id}',
                                    activity = '{activity}',
                                    store_type = '{store_type}'
            where store_program_id = '{store_program_id}'""")

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def master_planogram_insert(program_id, cd_ay, cd_sn, lht_ay, lht_sn, lhd_ay, lhd_sn, lhp_ay, lhp_sn, total_cases, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("""
    INSERT INTO master_planogram (program_id, cd_ay, cd_sn, lht_ay, lht_sn, lhd_ay, lhd_sn, lhp_ay, lhp_sn, total_cases) 
    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (program_id, cd_ay, cd_sn, lht_ay, lht_sn,
                                                lhd_ay, lhd_sn, lhp_ay, lhp_sn, total_cases))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def master_planogram_update(program_id, cd_ay, cd_sn, lht_ay, lht_sn, lhd_ay, lhd_sn, lhp_ay, lhp_sn, total_cases, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""update master_planogram set cd_ay = '{cd_ay}',
                                    cd_sn= '{cd_sn}',
                                    lht_ay = '{lht_ay}',
                                    lht_sn = '{lht_sn}',
                                    lhd_ay = '{lhd_ay}',
                                    lhd_sn = '{lhd_sn}',
                                    lhp_ay = '{lhp_ay}',
                                    lhp_sn = '{lhp_sn}',
                                    total_cases = '{total_cases}'
            where program_id = '{program_id}'""")

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def item_approval_insert(code, store_price, connection_pool, store_type_input):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(f"INSERT INTO {store_type_input}.item_approval (code, store_price) values (%s,%s)",
                   (code, store_price))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)

def item_approval_update(code, store_price, connection_pool, store_type_input):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"""update {store_type_input}.item_approval set store_price = '{store_price}'
            where code = '{code}'""")

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def inventory_insert(code, on_hand, connection_pool):

    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute("INSERT INTO inventory (code, on_hand) values (%s,%s)",
                   (code, on_hand))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def inventory_update(code, on_hand, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"update inventory set on_hand = '{on_hand}' where code = '{code}'")

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def size_table_insert(code, size, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO item_size (code, size) values (%s,%s)",
        (code,size))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def size_table_update(code, size, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"update item_size set size = '{size}' where code = '{code}'")

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def year_week_verify_insert(store_year, store_week, store_type_input, connection_pool):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    cursor.execute(
        f"INSERT INTO {store_type_input}.year_week_verify (store_year, store_week, store_type) values (%s,%s,%s)",
        (store_year, store_week, store_type_input))

    connection.commit()
    cursor.close()
    connection_pool.putconn(connection)


def insert_execute_batch(connection_pool, df, table_name, page_size=100):
    """
    Using psycopg2.extras.execute_batch() to insert the dataframe
    """

    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))

    number_of_columns = len(list(df.columns))

    # Create a list of tuples from the dataframe values
    tuples_list = [tuple(x) for x in df.to_numpy()]

    values = '%%s'

    for i in range(number_of_columns - 1):
        values = values + ',%%s'

    # SQL query to execute
    query = f"INSERT INTO %s(%s) VALUES({values})" % (table_name, cols)

    connection = connection_pool.getconn()
    cursor = connection.cursor()

    try:
        extras.execute_batch(cursor, query, tuples_list, page_size)
        connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
        cursor.close()

    print("Insert execute_batch() done")
    cursor.close()