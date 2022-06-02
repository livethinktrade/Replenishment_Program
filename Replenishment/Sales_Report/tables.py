from psycopg2 import pool
import pandas as pd
import pandas.io.sql as psql



class Tables:

    def __init__(self, store_type_input, store_setting):

        self.store_type_input = store_type_input

        self.store_setting = store_setting

    def on_hand(self):
        pass


    #created deliv table using only 2 years of data and using parameter from the store settings
    #create sd_combo using deliv table

    def available_space(self):
        pass


