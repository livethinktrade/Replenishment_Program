with
    ytd_sales as (
            select store_number, sum(sales) as ytd_dollar, sum(qty) as ytd_qty
            from kroger_central.sales2
            where store_year = 2022
            group by store_number
            order by store_number
            ),

    current_wk_sales as (
            select store_number,  sum(sales) as current_wk_sales
            from kroger_central.sales2
            where store_year = 2022 and store_week = 32
            group by store_number
            order by store_number
            ),

    prev_wk_sales as (
            select store_number, sum(sales) as prev_wk_sales
            from kroger_central.sales2
            where store_year = 2022 and store_week = 31
            group by store_number
            order by store_number
    )

select ytd_sales.store_number, ytd_dollar, ytd_qty, current_wk_sales, prev_wk_sales from ytd_sales
left join current_wk_sales on ytd_sales.store_number = current_wk_sales.store_number
left join prev_wk_sales on ytd_sales.store_number =  prev_wk_sales.store_number



w
