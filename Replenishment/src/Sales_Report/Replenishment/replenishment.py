import pandas as pd
import pandas.io.sql as psql
from src.Sales_Report.Reports.reportsdata import ReportsData
from config.DbConfig import *


class Restock:

    def __init__(self, store_type_input, store_setting):

        self.store_type_input = store_type_input

        self.store_setting = store_setting

        self.carded_1 = store_setting.loc['Carded-1', 'values']

        self.carded_2 = store_setting.loc['Carded-2', 'values']

        self.carded_3 = store_setting.loc['Carded-3', 'values']

        self.top_1 = store_setting.loc['Top-1', 'values']

        self.top_2 = store_setting.loc['Top-2', 'values']

        self.top_3 = store_setting.loc['Top-3', 'values']

        self.dress_1 = store_setting.loc['Dress-1', 'values']

        self.dress_2 = store_setting.loc['Dress-2', 'values']

        self.dress_3 = store_setting.loc['Dress-3', 'values']

        self.transition_season = store_setting.loc['Transition_Season', 'values']

        self.max_time_replenish = store_setting.loc['max_time_replenish', 'values']

        self.case_qty_replenishment_threshold = store_setting.loc['case_qty_replenishment_threshold', 'values']

        self.cases_on_hand_setting = store_setting.loc['cases_on_hand', 'values']

        self.return_percentage = self.store_setting.loc['Return_Pecentage', 'values']

        self.connection = engine_pool_connection()

        reports = ReportsData(self.store_type_input, self.store_setting)

        self.on_hands = reports.on_hands()

    def replenishment(self):

        data = {'Carded': [self.carded_1, self.carded_2, self.carded_3],
                'Long Hanging Top': [self.top_1, self.top_2, self.top_3],
                'Long Hanging Dress': [self.dress_1, self.dress_2, self.dress_3]
                }

        # Creates pandas DataFrame.
        available_space_replen_table = pd.DataFrame(data, index=[1, 2, 3])

        # takes df created from object and grabs only certain columns and does a group by function.
        # Also unindeed columns store & display and assigns new df to on_hand_display_size

        sd_pivot = self.on_hands[['store', 'display_size', 'case_qty']]

        sd_pivot = sd_pivot.groupby(['store', 'display_size']).sum()

        sd_pivot.reset_index(inplace=True)

        sd_pivot['store'] = sd_pivot.store.astype(int)

        sd_pivot.set_index(['store', 'display_size'])

        on_hands_display_size = sd_pivot
        on_hands_display_size['display_space_capacity'] = 0

        store_total_capacity = psql.read_sql(f"""

            select store_id as store, sum(total_cases) as total_capacity
            from {self.store_type_input}.store_program
            inner join master_planogram on {self.store_type_input}.store_program.program_id = master_planogram.program_id
            where activity = 'ACTIVE'
            group by store_id
            order by sum(total_cases)

            """, self.connection)

        store_total_capacity = store_total_capacity.set_index('store')

        store_display_size_capacity = psql.read_sql(f""" 
            
            select store_id, "public".master_planogram.*
            from {self.store_type_input}.store_program
            inner join master_planogram on {self.store_type_input}.store_program.program_id = master_planogram.program_id
            where activity = 'ACTIVE'
            order by store_id
            """, self.connection)

        # establishing a df that can be manipulated later when calculating space based off of store, display_size, and season

        store_display_size_season_capacity = store_display_size_capacity.copy(deep=True)
        store_display_size_season_capacity = store_display_size_capacity[['store_id', 'cd_ay', 'cd_sn', 'lht_ay',
                                                                          'lht_sn', 'lhd_ay', 'lhd_sn', 'lhp_ay', 'lhp_sn']]
        store_display_size_season_capacity = store_display_size_capacity.groupby(by='store_id').sum()


        # combining the AY & seasonal display size space to find out how much capacity
        store_display_size_capacity['Carded'] = store_display_size_capacity['cd_ay'] + store_display_size_capacity['cd_sn']
        store_display_size_capacity['Long Hanging Top'] = store_display_size_capacity['lht_ay'] + store_display_size_capacity['lht_sn']
        store_display_size_capacity['Long Hanging Dress'] = store_display_size_capacity['lhd_ay'] + store_display_size_capacity['lhd_sn']
        store_display_size_capacity['Long Hanging Pants'] = store_display_size_capacity['lhp_ay'] + store_display_size_capacity['lht_sn']

        store_display_size_capacity = store_display_size_capacity[['store_id', 'Carded', 'Long Hanging Top', 'Long Hanging Dress', 'Long Hanging Pants']]
        store_display_size_capacity = store_display_size_capacity.groupby(['store_id']).sum()
        # store_display_size_capacity = store_display_size_capacity.set_index('store_id')

        store_notes = psql.read_sql(f'select * from {self.store_type_input}.store', self.connection)

        no_program = []
        i = 0

        while i < len(on_hands_display_size):

            store = on_hands_display_size.iloc[i, 0]
            display_size = on_hands_display_size.iloc[i, 1]

            try:

                capacity = store_display_size_capacity.loc[store, f'{display_size}']

                on_hands_display_size.iloc[i, 3] = capacity

            except KeyError:

                no_program.append(store)

            finally:

                i += 1

        print('No Programs have been established for these stores listed:\n\n', no_program)

        on_hands_display_size['space available'] = on_hands_display_size['display_space_capacity'] - on_hands_display_size[
            'case_qty']


        replenishment = pd.DataFrame(
            columns=['initial', 'store', 'item', 'case', 'notes', 'case_qty', 'display_size'])

        replenishment_reasons = pd.DataFrame(columns=['store', 'display_size', 'item_group_desc',
                                                      'case_qty', 'reason'])

        # Filter out any items that have high return percentage. variable defined in store_settings

        on_hands = self.on_hands

        on_hands['return_ratio'] = (on_hands['credit'] * -1) / on_hands['deliveries']

        on_hands = on_hands[(on_hands['return_ratio'] < self.return_percentage)]

        # Next section is using the potential_replenishment table and see if it passes the avail space replen table
        i = 0

        # this next section is defining a df for the case qty total for all of the display_sizes. Neccessary check so a store
        # does not get overshipped with product

        # # store_capacity_total = store_capacity[['total_capacity']]
        # store_capacity_total.index = store_capacity_total.index.rename('store')

        on_hands_store_case_total = on_hands_display_size[['store', 'case_qty']]
        on_hands_store_case_total = on_hands_store_case_total.groupby(['store']).sum()
        on_hands_store_case_total = pd.merge(on_hands_store_case_total, store_total_capacity, how='inner', on=['store'])
        on_hands_store_case_total['space available'] = on_hands_store_case_total['total_capacity'] - \
                                                       on_hands_store_case_total['case_qty']

        # set variable to a new df that will produce a df that gives the case qty after replenishment
        on_hands_after_replen = on_hands[['store', 'item_group_desc', 'display_size', 'case_qty']]
        on_hands_after_replen = on_hands_after_replen.set_index(['store', 'item_group_desc'])

        # finding the on hands for each season per display size per store
        on_hands_display_size_season = self.on_hands[['store', 'display_size', 'season', 'case_qty']]

        i = 0

        while i < len(on_hands_display_size_season):

            season = on_hands_display_size_season.loc[i, 'season']

            if season == 'AY':
                on_hands_display_size_season.loc[i, 'season_combine'] = season
            else:
                on_hands_display_size_season.loc[i, 'season_combine'] = 'SN'

            i += 1

        on_hands_display_size_season = on_hands_display_size_season.groupby(by=['store', 'display_size', 'season_combine']).sum()

        on_hands_display_size_season = on_hands_display_size_season.reset_index()

        # find the allocated space for each season per display size per store

        i = 0

        display_size_dict = {'Carded': 'cd',
                             'Long Hanging Top': 'lht',
                             'Long Hanging Dress': 'lhd',
                             'Long Hanging Pants': 'lhp',
                             'AY': '_ay',
                             'SN': '_sn'}

        while i < len(on_hands_display_size_season):

            store = on_hands_display_size_season.loc[i, 'store']
            display_size = on_hands_display_size_season.loc[i, 'display_size']
            season = on_hands_display_size_season.loc[i, 'season_combine']

            display_size = display_size_dict[f'{display_size}']
            season = display_size_dict[f'{season}']

            search_column = display_size + season

            try:

                 on_hands_display_size_season.loc[i, 'space_allocated'] = store_display_size_season_capacity.loc[store, search_column]

            except KeyError:

                on_hands_display_size_season.loc[i, 'space_allocated'] = 0

            i += 1

        on_hands_display_size_season['space_available'] = on_hands_display_size_season['space_allocated'] - on_hands_display_size_season['case_qty']

        potential_replenishment = on_hands_display_size_season[on_hands_display_size_season['space_available'] >= 0]
        potential_replenishment = potential_replenishment.sort_values(by=['space_available'], ascending=False)
        potential_replenishment = potential_replenishment.reset_index()
        potential_replenishment = potential_replenishment[
            ['store', 'display_size', 'season_combine', 'case_qty', 'space_allocated', 'space_available']]

        i = 0

        while i < len(potential_replenishment):

            store = potential_replenishment.loc[i, 'store']

            display_size = potential_replenishment.loc[i, 'display_size']

            space_capacity = potential_replenishment.loc[i, 'space_allocated']

            # space available per display size per season
            space_available = potential_replenishment.loc[i, 'space_available']

            season = potential_replenishment.loc[i, 'season_combine']
            season = self.season_identifier(season)

            # establish variables for store notes and division initials
            notes = store_notes[store_notes['store_id'] == store]
            notes = notes.reset_index(drop=True)
            initial = notes.loc[0, 'initial']
            notes = notes.loc[0, 'notes']

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
                # random number picked so the space availe if statement will fail and give a rejection reason
                available_space_threshold = 10

            # checking to see if we can replenish or not
            if space_available >= available_space_threshold:

                # passes the threshold test now it filters the sd combo table (aka on hands table) to
                # the store and display size and then selects the items with the lowest on hand

                on_hands_filter = on_hands[
                    (on_hands['store'] == store) &
                    (on_hands['display_size'] == f'{display_size}') &
                    (on_hands['season'] == f'{season}')]

                on_hands_filter = on_hands_filter.reset_index(drop=True)

                x = 0
                times_replenish = 0

                if len(on_hands_filter) <= 1:

                    while len(on_hands_filter) < 1 and times_replenish < self.max_time_replenish and \
                            space_available >= available_space_threshold and space_available_entire_program > 0:

                        potential_initial_order = self.initial_order(on_hands_filter, display_size, season)

                        recommendation = potential_initial_order['recommendation']

                        item_group_desc1 = potential_initial_order['new_item']

                        if recommendation == 'yes':

                            notes_verification = self.check_notes(notes)

                            if not notes_verification:
                                replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                      'case_qty': f'0',
                                                                                      'display_size': f'{display_size}',
                                                                                      'item_group_desc': f'{item_group_desc1}',
                                                                                      'reason': f'{notes}'},
                                                                                     ignore_index=True)

                                break

                            else:

                                #  code necessary to prevent shipping more than the max num of cases allowed and over shipping stores product
                                times_replenish += 1
                                potential_replenishment.loc[i, 'space_available'] = potential_replenishment.loc[
                                                                                        i, 'space_available'] - 1
                                space_available = potential_replenishment.loc[i, 'space_available']

                                on_hands_store_case_total.loc[store, 'space available'] = \
                                    on_hands_store_case_total.loc[
                                        store, 'space available'] - 1

                                space_available_entire_program = on_hands_store_case_total.loc[
                                    store, 'space available']


                                # insert into on hands after replen


                                # append to replenishment df after redefining variables

                                replenishment = replenishment.append({'initial': f'{initial}',
                                                                      'store': f'{store}',
                                                                      'item': f'{item_group_desc1}',
                                                                      'case': 1,
                                                                      'notes': f'{notes}',
                                                                      'case_qty': f'{0}',
                                                                      'display_size': f'{display_size}'},
                                                                     ignore_index=True)

                                x += 1

                                replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                      'case_qty': f'{0}',
                                                                                      'display_size': f'{display_size}',
                                                                                      'item_group_desc': f'{item_group_desc1}',
                                                                                      'reason': 'Innitial Order Recomendation'},
                                                                                     ignore_index=True)

                        else:

                            replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                  'case_qty': f'{0}',
                                                                                  'display_size': f'{display_size}',
                                                                                  'item_group_desc': f'{item_group_desc1}',
                                                                                  'reason': f'No substitution found for {item_group_desc1} for {self.transition_season} season'},
                                                                                 ignore_index=True)

                            break

                else:

                    # while loop function is needed in the event the item with the lowest on hand is not less than 1,
                    # not approved, out of stock

                    # note to self in the future if checking or revising code.
                    # 3 conditions must be met to continue on the replenishment process and for the loop to work:
                    #      1) must be less than the max amount of times to replenish
                    #      2) must meet space available reqs for whatever display_size must meet threshold reqs
                    #         (based on the available_space_replen_table)
                    #      3) Space available for the entire program must be greater than 0

                    while x < len(on_hands_filter) and times_replenish < self.max_time_replenish and \
                            space_available >= available_space_threshold and space_available_entire_program > 0:

                        # check to see if the on hands for the lowest item is lower than what ever percentage wanted. if
                        # it is not then skip replenishment for this store and go to the next store on the potential
                        # replen list. will break loop because if the item with the lowest on hand is > than whatever
                        # percentage selected then all of the other item are >  as well case_qty_replenishment_threshold
                        # is basically how much of the case we have left on hand

                        case_qty = round(on_hands_filter.loc[x, 'case_qty'], 2)

                        # define variables that will be used to check if the item is approved or not
                        item_group_desc = on_hands_filter.iloc[x, 1]
                        season = on_hands_filter.iloc[x, 3]

                        if case_qty < self.case_qty_replenishment_threshold:

                            # calls for approval list from database. it will be filtering based off of item_group_desc
                            # variable defined select statement will produce columns code, item_group_desc and
                            # store_price with item < $999

                            approval_df = psql.read_sql(f"""

                                select {self.store_type_input}.item_approval.code, item_group_desc, type, style, store_price
                                from {self.store_type_input}.item_approval
                                inner join item_support2 on item_approval.code = item_support2.code

                                where store_price < 999 and item_group_desc = '{item_group_desc}'

                                """, self.connection)

                            support_info = psql.read_sql(f"select type, style from item_support2 where item_group_desc = '{item_group_desc}'", self.connection)
                            type = support_info.loc[0, 'type']
                            style = support_info.loc[0, 'style']

                            # since the query above calls for a list of approved items. if the list contains 1 item then
                            # the item is approved

                            if len(approval_df) >= 1:

                                # define variable to check inventory using the df generated from above
                                # will check to see if those codes's have enough inventory

                                inventory = psql.read_sql(f"""

                                    select item_group_desc, sum(on_hand) as on_hand, sum(on_hand)/max(case_size) as cases_on_hand
                                    from {self.store_type_input}.item_approval
                                    inner join item_support2 on {self.store_type_input}.item_approval.code = item_support2.code
                                    inner join inventory on {self.store_type_input}.item_approval.code = inventory.code

                                    where store_price < 999 and item_group_desc = '{item_group_desc}'
                                    group by item_group_desc, display_size, category, season, style

                                    """, self.connection)

                                if len(inventory) == 0:
                                    raise Exception("""\n\nINVENTORY DATA NEEDS TO BE IMPORTED\n\n""")

                                cases_on_hand = inventory.loc[0, 'cases_on_hand']

                                # checks to see if item is in stock using item group desc
                                if cases_on_hand >= self.cases_on_hand_setting:

                                    # passes if statement since item is in stock then append to replenishment

                                    notes_verification = self.check_notes(notes)

                                    if not notes_verification:

                                        replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                              'case_qty': f'{case_qty}',
                                                                                              'display_size': f'{display_size}',
                                                                                              'item_group_desc': f'{item_group_desc}',
                                                                                              'reason': f'{notes}'},
                                                                                             ignore_index=True)

                                        # if it contain temp hold, closed, do not ship, then break loop and go to next store

                                        break

                                    else:

                                        #  code necessary to prevent shipping more than the max num of
                                        #  cases allowed and over shipping stores product
                                        times_replenish += 1
                                        potential_replenishment.loc[i, 'space_available'] = potential_replenishment.loc[
                                                                                                i, 'space_available'] - 1
                                        space_available = potential_replenishment.loc[i, 'space_available']

                                        on_hands_store_case_total.loc[store, 'space available'] = \
                                        on_hands_store_case_total.loc[
                                            store, 'space available'] - 1

                                        space_available_entire_program = on_hands_store_case_total.loc[
                                            store, 'space available']

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



                                    replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                          'case_qty': f'{case_qty}',
                                                                                          'display_size': f'{display_size}',
                                                                                          'item_group_desc': f'{item_group_desc}',
                                                                                          'reason': f'failed inventory Case setting set to {self.cases_on_hand_setting} cases on hand = {cases_on_hand}'},
                                                                                         ignore_index=True)

                                    potential_substitute = self.initial_order(on_hands_filter, display_size, season,
                                                                              type=type, style=style)
                                    recommendation = potential_substitute['recommendation']
                                    item_group_desc1 = potential_substitute['new_item']

                                    if recommendation == 'yes':
                                        times_replenish += 1
                                        potential_replenishment.loc[i, 'space_available'] = potential_replenishment.loc[
                                                                                                i, 'space_available'] - 1
                                        space_available = potential_replenishment.loc[i, 'space_available']

                                        on_hands_store_case_total.loc[store, 'space available'] = \
                                            on_hands_store_case_total.loc[
                                                store, 'space available'] - 1

                                        space_available_entire_program = on_hands_store_case_total.loc[
                                            store, 'space available']

                                        # append to replenishment df after redefining variables

                                        replenishment = replenishment.append({'initial': f'{initial}',
                                                                              'store': f'{store}',
                                                                              'item': f'{item_group_desc1}',
                                                                              'case': 1,
                                                                              'notes': f'{notes}',
                                                                              'case_qty': f'{0}',
                                                                              'display_size': f'{display_size}'},
                                                                             ignore_index=True)

                                        x += 1

                                        replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                              'case_qty': f'{0}',
                                                                                              'display_size': f'{display_size}',
                                                                                              'item_group_desc': f'{item_group_desc1}',
                                                                                              'reason': f'Item substitued since {item_group_desc} failed inventory'},
                                                                                             ignore_index=True)

                                    else:
                                        replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                              'case_qty': f'{0}',
                                                                                              'display_size': f'{display_size}',
                                                                                              'item_group_desc': f'{item_group_desc1}',
                                                                                              'reason': f'No substitution found for {item_group_desc}'},
                                                                                             ignore_index=True)
                                        x += 1

                            else:

                                # if item is not approved then go to next item on the on_hands_filter list

                                replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                      'case_qty': f'{case_qty}',
                                                                                      'display_size': f'{display_size}',
                                                                                      'item_group_desc': f'{item_group_desc}',
                                                                                      'reason': 'failed approval'},
                                                                                     ignore_index=True)

                                # find substitute
                                potential_substitute = self.initial_order(on_hands_filter, display_size, season,
                                                                          type=type, style=style)
                                recommendation = potential_substitute['recommendation']
                                item_group_desc1 = potential_substitute['new_item']

                                if recommendation == 'yes':
                                    times_replenish += 1
                                    potential_replenishment.loc[i, 'space_available'] = potential_replenishment.loc[
                                                                                            i, 'space_available'] - 1
                                    space_available = potential_replenishment.loc[i, 'space_available']

                                    on_hands_store_case_total.loc[store, 'space available'] = \
                                        on_hands_store_case_total.loc[
                                            store, 'space available'] - 1

                                    space_available_entire_program = on_hands_store_case_total.loc[
                                        store, 'space available']

                                    # append to replenishment df after redefining variables

                                    replenishment = replenishment.append({'initial': f'{initial}',
                                                                          'store': f'{store}',
                                                                          'item': f'{item_group_desc1}',
                                                                          'case': 1,
                                                                          'notes': f'{notes}',
                                                                          'case_qty': f'{0}',
                                                                          'display_size': f'{display_size}'},
                                                                         ignore_index=True)

                                    x += 1

                                    replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                          'case_qty': f'{0}',
                                                                                          'display_size': f'{display_size}',
                                                                                          'item_group_desc': f'{item_group_desc1}',
                                                                                          'reason': f'Item substitued since {item_group_desc} failed approval'},
                                                                                         ignore_index=True)

                                else:
                                    replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                          'case_qty': f'{0}',
                                                                                          'display_size': f'{display_size}',
                                                                                          'item_group_desc': f'{item_group_desc1}',
                                                                                          'reason': f'No substitution found for {item_group_desc}'},
                                                                                         ignore_index=True)
                                    x += 1

                        else:

                            replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                                  'case_qty': f'{case_qty}',
                                                                                  'display_size': f'{display_size}',
                                                                                  'item_group_desc': f'{item_group_desc}',
                                                                                  'reason': f'case qty > threshold setting of {self.case_qty_replenishment_threshold * 100}%'},
                                                                                 ignore_index=True)
                            break

            else:

                replenishment_reasons = replenishment_reasons.append({'store': f'{store}',
                                                                      'display_size': f'{display_size}',
                                                                      'reason': f'''failed {display_size} space available threshold of {available_space_threshold}. space availilable for {display_size} @ {round(space_available, 2)} cases'''},
                                                                     ignore_index=True)

            i += 1

        self.connection.dispose()

        replenishment['store_name'] = replenishment['initial'] + replenishment['store'].astype(str)
        replenishment_reasons = replenishment_reasons.sort_values(by='store')
        replenishment_reasons = replenishment_reasons.reset_index(drop=True)
        on_hands_store_case_total = round(on_hands_store_case_total['space available'])

        return {'replenishment': replenishment,
                'on_hands_after_replenishment': on_hands_after_replen,
                'replenishment_reasons': replenishment_reasons,
                'on_hands_store_case_total': on_hands_store_case_total,
                'on_hands_display_size_season': on_hands_display_size_season
                }

    def initial_order(self, on_hands_filter, display_size, season, type=None, style=None):

        """

        :param on_hands_filter: dataframe
        :param display_size:
        :param season: str
        :param type: null
        :param style None

        :return: item group desc

        Method is used whenever a new item needs to be recommended
        """

        new_item = f"""
        
        select {self.store_type_input}.item_approval.code,item_group_desc, display_size, season, type, 
                style, on_hand, case_size, round(on_hand/case_size) as cases_on_hand
        from {self.store_type_input}.item_approval
        inner join item_support2 on {self.store_type_input}.item_approval.code = public.item_support2.code
        inner join inventory on {self.store_type_input}.item_approval.code = public.inventory.code
        where store_price < 999 and round(on_hand/case_size) > {self.cases_on_hand_setting} and 
              season = '{season}' and display_size = '{display_size}'
        order by on_hand desc
        """

        new_item_list = psql.read_sql(new_item, self.connection)

        if len(new_item_list) > 1:

            if len(on_hands_filter) <= 0:

                new_item = new_item_list.loc[0, 'item_group_desc']
                recommendation = 'yes'

            else:
                # substitutions or filling up space

                new_item_list_filtered = new_item_list[(new_item_list['season'] == f'{season}') &
                                                       (new_item_list['display_size'] == f'{display_size}') &
                                                       (new_item_list['type'] == f'{type}') &
                                                       (new_item_list['style'] == f'{style}')]

                new_item_list_filtered = new_item_list_filtered.reset_index(drop=True)

                x = 0
                recommendation = 'no'
                new_item_canidate = ''

                while x < len(new_item_list_filtered):

                    new_item_canidate = new_item_list_filtered.loc[x, 'item_group_desc']

                    length = on_hands_filter[on_hands_filter['item_group_desc'] == f'{new_item_canidate}']

                    if len(length) >= 1:
                        x += 1

                    else:
                        recommendation = 'yes'
                        break

                if recommendation == 'yes':
                    new_item = new_item_canidate
        else:
            new_item = ''
            recommendation = 'no'

        return {'new_item': new_item, 'recommendation': recommendation}

    def season_identifier(self, season):
        """
        method is necessary because the season value that will be returned will be used as the filter for the on hands
        replenishment.

        Takes in Season combine variable from potential replenishment table and then check to see if it is filtered
        by the right season

        """

        if season == 'AY':
            pass

        elif season == 'SN':
            season = self.transition_season

        else:
            raise Exception("Combine Season from potential Replenishment table should either be AY or SN")

        return season

    def check_notes(self, notes):

        """

        :param notes:
        :return: Boolean value
        """

        notes_list = ['TEMP HOLD', 'CLOSED', 'DO NOT SHIP', 'CALL IN ONLY', ' CLOSED',
                      'MASKS ONLY', ' MASKS ONLY', 'CALL IN ONLY, NO DRESSES',
                      'CALL INS ONLY, NO DRESSES', 'CALL INS ONLY']

        if notes in notes_list:
            status = False

        else:
            status = True

        return status




