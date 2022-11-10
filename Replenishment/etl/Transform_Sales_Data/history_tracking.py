import datetime

import psycopg2
import numpy as np

from psycopg2.extensions import register_adapter
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


class HistoryTracking:

    def __init__(self, store_type_input, transition_year, transition_season, connection, connection_pool):

        self.store_type_input = store_type_input
        self.connection = connection
        self.connection_pool = connection_pool
        self.transition_year = transition_year
        self.transition_season = transition_season

    def existing_program(self, store_program_id, store_id, program_id, activity, store_type, duplicate_check): # updating

        duplicate_check_store_id = duplicate_check.loc[0, 'store_id']
        duplicate_check_program_id = duplicate_check.loc[0, 'program_id']
        duplicate_check_activity = duplicate_check.loc[0, 'activity']
        duplicate_store_type = duplicate_check.loc[0, 'store_type']

        if duplicate_store_type != store_type:
            raise Exception(f"Row: {store_program_id} is not the same store_type as '{duplicate_store_type}' in the database")

        if duplicate_check_store_id != store_id:
            raise Exception(
                f"""
                
                Error Store Program ID: {store_program_id} in {self.store_type_input} store setting
                
                Update failed: store program ID and store number need to match wtih the data in the database
                
                If the store program is not active anymore then switch activity to inactive. 
                If there is a new program for a store a new row must be inserted along with a new store_prodgram_id    
                """)

        if (duplicate_check_program_id != program_id) or (duplicate_check_activity != activity):
            self.store_program_history_insert(store_program_id, store_id, program_id, activity, store_type)
            add_history = 1

        else:
            add_history = 0

        return add_history

    def new_program(self, store_program_id, store_id, program_id, activity, store_type):

        self.store_program_history_insert(store_program_id, store_id, program_id, activity, store_type)

        add_history = 1

        return add_history

    def on_hands_tracking(self):
        pass

    def store_program_history_insert(self, store_program_id, store_id, program_id, activity, store_type):

        today_date = datetime.datetime.now()
        today_date = today_date.date()

        connection = self.connection_pool.getconn()
        cursor = connection.cursor()
        cursor.execute(
            f""" INSERT INTO {self.store_type_input}.store_program_history (store_program_id, store_id, program_id, 
            activity, store_type, date_updated) values (%s,%s,%s,%s,%s,%s)""",
            (store_program_id, store_id, program_id, activity, store_type, today_date))

        connection.commit()
        cursor.close()
        self.connection_pool.putconn(connection)
