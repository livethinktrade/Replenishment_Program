import psycopg2
import pandas as pd
import pandas.io.sql as psql


def replenishment(store_type_input):
    connection = psycopg2.connect(database=f"{store_type_input}", user="postgres", password="winwin", host="localhost")



    #generates df that will give you a table that concist of stores that have available space

    space_available = psql.read_sql("""

        with colapse as(
                select  sd_combo.store, 
                        display_size, 
                        sum(case_qty) as case_qty,
                        carded,
                        long_hanging_top,
                        long_hanging_dress,
                        case
                            when display_size = 'Carded' then (carded - sum(case_qty))
                            when display_size = 'Long Hanging Top' then (long_hanging_top - sum(case_qty))
                            when display_size = 'Long Hanging Dress' then (long_hanging_dress - sum(case_qty))
                            end as available_space

                from sd_combo
                inner join case_capacity on sd_combo.store = case_capacity.store

                /*THIS LINE IS DEPENDENT ON THE SEASON WE ARE CURRENTLY IN, IF FW THEN SWITCH SS TO FW*/
                where season = 'AY' or season = 'SS'

                group by sd_combo.store ,display_size, long_hanging_top, long_hanging_dress, carded
                order by sd_combo.store asc)
            
        select  store, display_size, round(available_space) as space_available 
        from colapse
        group by store, display_size, available_space
        Having round(available_space) > 0

    """, connection)
    space_available

    number_of_stores_need_replenish = len(space_available)

    #Creates table for final replenishment order and recommendations
    replenishment = pd.DataFrame(columns = ['initial', 'store', 'item','case','notes','case_qty','display_size'])

    i=0

    
    # takes the df that concist of stores and available space and
    # finds how many items the store that needs replenishing has in their stores number is used to helf find 

    while i < number_of_stores_need_replenish:
        

        # this code is selecting  the store and dislay size and the number of available space then looking at the sd_combo table to look for recomendation 
        store = space_available.iloc[i,0]
        display_size = space_available.iloc[i,1]
        space = space_available.iloc[i,2]

        
        store_on_hand = psql.read_sql(f"""


        select initial,
                sd_combo.store, 
                sd_combo.item_group_desc, 
                case_qty,
                sd_combo.display_size, 
                sd_combo.season, 
                max(availability) as in_stock, 
                notes
        from sd_combo

        inner join item_support on sd_combo.item_group_desc = item_support.item_group_desc
        inner join case_capacity on case_capacity.store = sd_combo.store

        where sd_combo.store = {store} and
            sd_combo.display_size = '{display_size}'

        group by initial,
                sd_combo.store, 
                sd_combo.item_group_desc, 
                case_qty,
                sd_combo.display_size, 
                sd_combo.season, notes
                
        order by case_qty

        """, connection)
        
        
        #selecting items to put in the final replenishment table

        number_of_items_in_store = len(store_on_hand)

        x = 0
        while space > 0:

            if x < number_of_items_in_store:

                initial = store_on_hand.iloc[x,0]
                store = store_on_hand.iloc[x,1]
                item = store_on_hand.iloc[x,2]
                case_qty = store_on_hand.iloc[x,3]
                display_size = store_on_hand.iloc[x,4]
                in_stock = store_on_hand.iloc[x,6]
                notes = store_on_hand.iloc[x,7]

                #checking to see if item is in stock if not then loops around to the next item and repeats this step
                if in_stock > 0:
                    replenishment = replenishment.append({'initial' : f'{initial}', 
                                                            'store' : f'{store}', 
                                                            'item' : f'{item}',
                                                            'case' : 1,
                                                            'notes': f'{notes}',
                                                            'case_qty': f'{case_qty}',
                                                            'display_size': f'{display_size}'}, 
                        ignore_index = True)

                    space-=1

                x+=1 
            
            else:
                space-=1

        i+=1

    replenishment['store'] = replenishment['initial'] + replenishment['store']
    replenishment.pop('initial')

    return replenishment


