import psycopg2
import pandas as pd
import pandas.io.sql as psql
from Sales_Report.Reports.reportsdata import ReportsData


def replenishment(store_type_input, store_setting):


    # creates a df for the Available space Replenishment table (note to self -may move this later to the main class )
    # no need fo code below because store settings will be changed below
    # store_setting = store_setting.set_index(0)
    # store_setting = store_setting.rename(columns={1: 'values'})

    carded_1 = store_setting.loc['Carded-1', 'values']

    carded_2 = store_setting.loc['Carded-2', 'values']

    carded_3 = store_setting.loc['Carded-3', 'values']

    top_1 = store_setting.loc['Top-1', 'values']

    top_2 = store_setting.loc['Top-2', 'values']

    top_3 = store_setting.loc['Top-3', 'values']

    dress_1 = store_setting.loc['Dress-1', 'values']

    dress_2 = store_setting.loc['Dress-2', 'values']

    dress_3 = store_setting.loc['Dress-3', 'values']

    data = {'Carded': [carded_1, carded_2, carded_3],
            'Long Hanging Top': [top_1, top_2, top_3],
            'Long Hanging Dress': [dress_1, dress_2, dress_3]
            }

    # Creates pandas DataFrame.
    available_space_replen_table = pd.DataFrame(data, index=[1,
                                                             2,
                                                             3])

    connection = psycopg2.connect(database=f"Grocery", user="postgres", password="winwin", host="localhost")

    # this section of the code establishes the on hands by display size

    # creates the object to get the sd_combo table and then sorts the collumns in order (store, displaysize, case qty)
    reports = ReportsData(store_type_input, store_setting)

    on_hands = reports.on_hands()

    # takes df created from object and grabs only certain columns and does a group by function.
    # Also unindeed columns store & display and assigns new df to on_hand_display_size

    sd_pivot = on_hands[['store', 'display_size', 'case_qty']]

    sd_pivot = sd_pivot.groupby(['store', 'display_size']).sum()

    sd_pivot.reset_index(inplace=True)

    sd_pivot['store'] = sd_pivot.store.astype(int)

    sd_pivot.set_index(['store', 'display_size'])

    on_hands_display_size = sd_pivot

    # section of the code creates table to find the store capacity for each store  only grabs programs that are active
    store_capacity = psql.read_sql(f"""

        select store.store_id, carded, long_hanging_top, long_hanging_dress, initial, notes, activity
        from {store_type_input}.store_program
        inner join master_planogram on {store_type_input}.store_program.program_id = master_planogram.program_id
        inner join {store_type_input}.store on {store_type_input}.store.store_id = store_program.store_id
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

    on_hands_display_size['space_capacity'] = 0

    on_hand_len = len(on_hands_display_size)

    i = 0
    no_program = []

    # matching the two tables together based on display size. code will show the on hand for the display size and
    # then it will show the allocated program space capacity in another column. It will also produce a list where
    # any stores that have an on_hand but does not have a program (this happens because either  it is an old store or
    # store decided to take off of program)

    while i < on_hand_len:

        store = on_hands_display_size.iloc[i, 0]
        display_size = on_hands_display_size.iloc[i, 1]

        try:

            capacity = store_capacity.loc[store, f'{display_size}']

            on_hands_display_size.iloc[i, 3] = capacity

        except KeyError:

            no_program.append(store)


        finally:

            i += 1

    print('No Programs have been established for these stores listed:\n\n', no_program)

    on_hands_display_size['space available'] = on_hands_display_size['space_capacity'] - on_hands_display_size[
        'case_qty']

    potential_replenishment = on_hands_display_size[on_hands_display_size['space available'] >= 0]
    potential_replenishment = potential_replenishment.reset_index()
    potential_replenishment = potential_replenishment[
        ['store', 'display_size', 'case_qty', 'space_capacity', 'space available']]

    # potential_replenishment['rejection_reason'] = potential_replenishment['rejection_reaason'] = 0

    replenishment = pd.DataFrame(
        columns=['initial', 'store', 'item', 'case', 'notes', 'case_qty', 'display_size'])

    replenishment_reasons = pd.DataFrame(columns=['store', 'display_size', 'item_group_desc',
                                                  'case_qty', 'reason'])

    # Filter out any items that have high return percentage. variable defined in store_settings

    on_hands['return_ratio'] = (on_hands['credit'] * -1) / on_hands['deliveries']

    return_percentage = store_setting.loc['Return_Pecentage', 'values']

    on_hands = on_hands[(on_hands['return_ratio'] < return_percentage)]

    # Next section is using the potential_replenishment table and see if it passes the avail space replen table
    i = 0

    # this next section is defining a df for the case qty total for all of the display_sizes. Neccessary check so a store
    # does not get overshipped with product

    store_capacity_total = store_capacity[['total_capacity']]
    store_capacity_total.index = store_capacity_total.index.rename('store')

    on_hands_store_case_total = on_hands_display_size[['store', 'case_qty']]
    on_hands_store_case_total = on_hands_store_case_total.groupby(['store']).sum()
    on_hands_store_case_total = pd.merge(on_hands_store_case_total, store_capacity_total, how='inner', on=['store'])
    on_hands_store_case_total['space available'] = on_hands_store_case_total['total_capacity'] - \
                                                   on_hands_store_case_total['case_qty']

    # set variable to a new df that will produce a df that gives the case qty after replenishment
    on_hands_after_replen = on_hands[['store', 'item_group_desc', 'display_size', 'case_qty']]
    on_hands_after_replen = on_hands_after_replen.set_index(['store', 'item_group_desc'])

    # replenishment process code

    while i < len(potential_replenishment):

        store = potential_replenishment.loc[i, 'store']

        display_size = potential_replenishment.loc[i, 'display_size']

        space_capacity = potential_replenishment.loc[i, 'space_capacity']

        # space available per display size
        space_available = potential_replenishment.loc[i, 'space available']

        # space available for the entire program. calculated by sum qty's of (carded, top, dress) subtract from total space

        try:

            space_available_entire_program = on_hands_store_case_total.loc[store, 'space available']

        except KeyError:

            space_available_entire_program = 0

        # this line is necessary because the value from the space_capcity will be used as an indexer to reference the
        # avialable space replen table

        if space_capacity >= 3:
            space_capacity = 3

        try:
            available_space_threshold = available_space_replen_table.loc[space_capacity, f'{display_size}']

        except KeyError:
            #random number picked so the space availe if statement will fail and give a rejection reason
            available_space_threshold = 10

        # checking to see if we can replenish or not
        if space_available >= available_space_threshold:

            # passes the threshold test now it filters the sd combo table to the store and display size and then selects
            # the items with the lowest on hand

            on_hands_filter = on_hands[
                (on_hands['store'] == store) &
                (on_hands['display_size'] == f'{display_size}')]

            x = 0

            # while loop function is needed in the event the item with the lowest on hand is not less than 1,
            # not approved, out of stock

            max_time_replenish = store_setting.loc['max_time_replenish', 'values']

            times_replenish = 0

            # note to self in the futre if checking or revising code.
            # 3 condintions must be met to continue on the replenishment process and for the loop to work
            #                     1) must be less than the max amount of times to replenish
            #                     2) must meet space available for whatever display_size must meet threshold reqs
            #                     3) Space available for the entire program must be greater than 0

            while x < len(
                    on_hands_filter) and times_replenish < max_time_replenish and space_available >= available_space_threshold and space_available_entire_program > 0:

                case_qty = on_hands_filter.iloc[x, 9]

                # check to see if the on hands for the lowest item is lower than what ever percentage wanted. if it is not then skip replenshment
                # for this store and go to the next store on the potential replen list. will break loop because if the item
                # with the lowest on hand is > than whatever percentage selected then all of the other item are >  as well
                # case_qty_replenishment_threshold is basically how much of the case we have left on hand

                case_qty_replenishment_threshold = store_setting.loc['case_qty_replenishment_threshold', 'values']

                # define variables that will be used to check if the item is approved or not
                item_group_desc = on_hands_filter.iloc[x, 1]


                if case_qty < case_qty_replenishment_threshold:


                    # calls for approval list from database. it will be filtering based off of item_group_desc varaible defined
                    # select statement will produce columns code, item_group_desc and store_price with item < $999

                    approval_df = psql.read_sql(f""" 

                        select {store_type_input}.item_approval.code, item_group_desc, store_price
                        from {store_type_input}.item_approval
                        inner join item_support2 on item_approval.code = item_support2.code

                        where store_price < 999 and item_group_desc = '{item_group_desc}'

                        """, connection)

                    # since the query above calls for a list of approved items. if the list contains 1 item then
                    # the item is approved

                    if len(approval_df) >= 1:

                        # define variable to check inventory
                        # using the df generated from above will check to see if those codes's have enough inventory

                        inventory = psql.read_sql(f""" 

                            select item_group_desc, sum(on_hand) as on_hand, sum(on_hand)/max(case_size) as cases_on_hand
                            from {store_type_input}.item_approval
                            inner join item_support2 on {store_type_input}.item_approval.code = item_support2.code
                            inner join inventory on {store_type_input}.item_approval.code = inventory.code

                            where store_price < 999 and item_group_desc = '{item_group_desc}'
                            group by item_group_desc, display_size, category, season, style

                            """, connection)

                        if len(inventory) == 0:
                            print("""\n\nINVENTORY DATA NEEDS TO BE IMPORTED\n\n""")

                        cases_on_hand = inventory.loc[0, 'cases_on_hand']

                        cases_on_hand_setting = store_setting.loc['cases_on_hand', 'values']

                        if cases_on_hand >= cases_on_hand_setting:  # checks to see if item is in stock using item group desc

                            # passes if statement since item is in stock then append to replenishment

                            a = store_notes[store_notes['store_id'] == store]
                            a = a.reset_index(drop=True)
                            initial = a.loc[0, 'initial']
                            notes = a.loc[0, 'notes']

                            notes_list = ['TEMP HOLD', 'CLOSED', 'DO NOT SHIP', 'CALL IN ONLY']

                            if notes in notes_list:

                                replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                      'case_qty': f'{case_qty}',
                                                                                      'display_size': f'{display_size}',
                                                                                      'item_group_desc': f'{item_group_desc}',
                                                                                      'reason': f'{notes}'},
                                                                                     ignore_index=True)
                                # if it contain temp hold, closed, do not ship, then break loop and go to next store
                                break

                            else:

                                #  code necessary to prevent shipping more than the max num of cases allowed and over shipping stores product
                                times_replenish += 1
                                potential_replenishment.loc[i, 'space available'] = potential_replenishment.loc[
                                                                                        i, 'space available'] - 1
                                space_available = potential_replenishment.loc[i, 'space available']

                                on_hands_store_case_total.loc[store, 'space available'] = on_hands_store_case_total.loc[
                                                                                              store, 'space available'] - 1

                                space_available_entire_program = on_hands_store_case_total.loc[store, 'space available']

                                on_hands_after_replen.loc[(store, item_group_desc), 'case_qty'] = \
                                on_hands_after_replen.loc[(store, item_group_desc), 'case_qty'] + 1

                                # append to replenishment df after redefining variables

                                replenishment = replenishment.append({'initial': f'{initial}',
                                                                      'store': f'{store}',
                                                                      'item': f'{item_group_desc}',
                                                                      'case': 1,
                                                                      'notes': f'{notes}',
                                                                      'case_qty': f'{case_qty}',
                                                                      'display_size': f'{display_size}'},
                                                                     ignore_index=True)

                                x += 1

                                replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                      'case_qty': f'{case_qty}',
                                                                                      'display_size': f'{display_size}',
                                                                                      'item_group_desc': f'{item_group_desc}',
                                                                                      'reason': 'Item Replenished'},
                                                                                     ignore_index=True)

                        else:

                            x += 1

                            replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                  'case_qty': f'{case_qty}',
                                                                                  'display_size': f'{display_size}',
                                                                                  'item_group_desc': f'{item_group_desc}',
                                                                                  'reason': 'failed inventory'},
                                                                                 ignore_index=True)


                    else:  # if item is not approved then go to next item on the on_hands_filter list

                        replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                              'case_qty': f'{case_qty}',
                                                                              'display_size': f'{display_size}',
                                                                              'item_group_desc': f'{item_group_desc}',
                                                                              'reason': 'failed approval'},
                                                                             ignore_index=True)
                        x += 1

                else:

                    replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                          'case_qty': f'{case_qty}',
                                                                          'display_size': f'{display_size}',
                                                                          'item_group_desc': f'{item_group_desc}',
                                                                          'reason': f'case qty > threshold setting of {case_qty_replenishment_threshold*100}%'},
                                                                         ignore_index=True)
                    break

        else:

            replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                  'display_size': f'{display_size}',
                                                                  'reason': f'''failed {display_size} space available threshold of {available_space_threshold}. space availilable for {display_size} @ {round(space_available,2)} cases'''},
                                                                 ignore_index=True)

        i += 1

    replenishment['store_name'] = replenishment['initial'] + replenishment['store'].astype(str)
    return replenishment, on_hands_after_replen, replenishment_reasons




