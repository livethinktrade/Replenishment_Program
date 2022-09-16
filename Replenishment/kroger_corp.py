import pandas as pd
import psycopg2
import numpy as np
import DbConfig
from psycopg2.extensions import register_adapter, AsIs

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
import os
from store_info import Replenishment
from Update.Transform_Sales_Data.transform import TransformData
import pandas.io.sql as psql

store_type = 'kroger_central'

kroger = ['kroger_dallas',
          'kroger_central',
          'kroger_delta',
          'kroger_michigan',
          'kroger_columbus',
          'kroger_atlanta',
          'kroger_cincinatti',
          'kroger_dillons',
          'kroger_king_soopers',
          'kroger_louisville',
          'kroger_nashville']

for store_type in kroger:
    with DbConfig.EnginePoolDB() as connection:
        df = psql.read_sql(f"""

        with
            ytd_sales as (
                    select store_number, sum(sales) as ytd_dollar, sum(qty) as ytd_qty
                    from {store_type}.sales2
                    where store_year = 2022
                    group by store_number
                    order by store_number
                    ),

            current_wk_sales as (
                    select store_number,  sum(sales) as current_wk_sales
                    from {store_type}.sales2
                    where store_year = 2022 and store_week = 32
                    group by store_number
                    order by store_number
                    ),

            prev_wk_sales as (
                    select store_number, sum(sales) as prev_wk_sales
                    from {store_type}.sales2
                    where store_year = 2022 and store_week = 31
                    group by store_number
                    order by store_number
            )

        select ytd_sales.store_number, ytd_dollar, ytd_qty, current_wk_sales, prev_wk_sales from ytd_sales
        left join current_wk_sales on ytd_sales.store_number = current_wk_sales.store_number
        left join prev_wk_sales on ytd_sales.store_number =  prev_wk_sales.store_number

    """, connection)

    df.to_excel(f'{store_type}.xlsx',
                sheet_name=f"{store_type}",
                index=False,
                columns=('store_number', 'ytd_dollar', 'ytd_qty', 'current_wk_sales', 'prev_wk_sales'))