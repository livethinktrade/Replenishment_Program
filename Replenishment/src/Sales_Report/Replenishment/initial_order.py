from src.Sales_Report.Replenishment.replenishment import *


def initial_order(store_type_input, store_setting):

    connection = psycopg2.connect(database=f"test", user="postgres", password="winwin", host="localhost")

    # getting the on hands after replenishment

    restock = Restock(store_type_input, store_setting)

    replen = restock.replenishment()

    on_hands_after_replen = replen['on_hands_after_replenishment']

    # establish on hands for display size for each store
    on_hands = on_hands_after_replen.reset_index()

    sd_pivot = on_hands[['store', 'display_size', 'case_qty']]

    sd_pivot = sd_pivot.groupby(['store', 'display_size']).sum()

    sd_pivot.reset_index(inplace=True)

    sd_pivot['store'] = sd_pivot.store.astype(int)

    sd_pivot.set_index(['store', 'display_size'])

    on_hands_display_size = sd_pivot

    # establish on hands for store disregarding
    on_hands = on_hands_after_replen.reset_index()

    sd_pivot = on_hands[['store', 'case_qty']]

    sd_pivot = sd_pivot.groupby(['store']).sum()

    sd_pivot.reset_index(inplace=True)

    sd_pivot['store'] = sd_pivot.store.astype(int)

    sd_pivot.set_index(['store'])

    on_hands_store = sd_pivot

    # section of the code creates table to find the store capacity for each store  only grabs programs that are active
    store_capacity = psql.read_sql(f"""

        select {store_type_input}.store.store_id, carded, long_hanging_top, long_hanging_dress, initial, notes, activity
        from {store_type_input}.store_program
        inner join master_planogram on {store_type_input}.store_program.program_id = master_planogram.program_id
        inner join {store_type_input}.store on {store_type_input}.store.store_id = {store_type_input}.store_program.store_id
        where activity = 'ACTIVE'

        order by store_id""", connection)

    store_notes = store_capacity

    # store capacity is a df that contains 4 columns (carded, long hanging top, long dress, and total capacity)
    # store number is the indexer for this df

    store_capacity = store_capacity.fillna(0)
    store_capacity = store_capacity.groupby(['store_id']).sum()

    store_capacity['total_capacity'] = store_capacity['carded'] + store_capacity['long_hanging_top'] + store_capacity[
        'long_hanging_dress']

    # this next section is combining the data from the two data frames (store capacity & on_hand_display_size)
    # from the two stores together so we can ge the space available.

    # renaming columns for store_capcity so code for matching the two tables together would work
    store_capacity = store_capacity.rename(columns={'carded': 'Carded',
                                                    'long_hanging_top': 'Long Hanging Top',
                                                    'long_hanging_dress': 'Long Hanging Dress'
                                                    })

    # matching 2 tables for entire store capacity
    on_hands_store['store_total_capacity'] = 0
    i = 0
    no_program_store = []

    while i < len(on_hands_store):

        store = on_hands_store.iloc[i, 0]

        try:

            capacity = store_capacity.loc[store, 'total_capacity']

            on_hands_store.iloc[i, 2] = capacity

        except KeyError:

            no_program_store.append(store)


        finally:

            i += 1

    on_hands_store['space available'] = on_hands_store['store_total_capacity'] - on_hands_store['case_qty']

    display_space = store_capacity
    display_space = display_space.drop(columns='total_capacity')
    display_space = display_space.stack()
    display_space = display_space.to_frame()
    display_space = display_space.reset_index()
    display_space = display_space.rename(columns={'level_1': 'display_size',
                                                  0: 'space allocated',
                                                  'store_id': 'store'})

    display_space = pd.merge(display_space, on_hands_display_size, how='left', on=['store', 'display_size'])

    display_space['case_qty'] = display_space['case_qty'].fillna(0)

    display_space['space available'] = display_space['space allocated'] - display_space['case_qty']

    on_hands_store = on_hands_store.sort_values(by=['space available'], ascending=False)

    store_space = on_hands_store.set_index('store')

    display_space['store space available'] = 0

    i = 0

    while i < len(display_space):
        store = display_space.iloc[i, 0]

        try:

            store_space_available = store_space.loc[store, 'space available']

            display_space.loc[i, 'store space available'] = store_space_available

        except KeyError:
            print(f'look into store {store} something is off that needs to be fixed')


        i += 1

    display_space = display_space.sort_values(by=['store space available', 'space available'], ascending=False)

    display_space = display_space.rename(columns={'space available': 'display space available'})

    return on_hands_store, display_space