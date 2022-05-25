with date as ( 
	select distinct store_number 
	from sales
	order by store_number asc),

    current_week as (

        select store_number, store_year, store_week, sum(sales) as sales 
        from sales
        where store_year = 2022 and
            store_week = 10
        
        group by store_number, store_year, store_week
        
        order by store_number asc),

    previous_week as (

        select store_number, store_year, store_week, sum(sales) as sales 
        from sales
        where store_year = 2022 and
            store_week = 9
        
        group by store_number, store_year, store_week
        
        order by store_number asc),

    previous_year_week as (
        select store_number, store_year, store_week, sum(sales) as sales 
        from sales
        where store_year = 2021 and
            store_week = 10
        
        group by store_number, store_year, store_week
        
        order by store_number asc),

    current_ytd_week as (

        select store_number, store_year, sum(sales) as sales 
        from sales
        where store_year = 2022 and
            store_week <= 10
        
        group by store_number, store_year
        
        order by store_number asc),


    previous_ytd_week as (
    

        select store_number, store_year, sum(sales) as sales 
        from sales
        /*looks for previous year so maybe max -1 when finding innitial start for python program*/
        where store_year = 2021 and
            store_week <= 10
        
        group by store_number, store_year
        
        order by store_number asc)

select
    date.store_number,
    current_week.sales as current_week,
    previous_week.sales as previous_week,
    /*WOW sales % */
    case
        when current_week.sales < 0 or previous_week.sales <= 0
            then NULL
        when current_week.sales >= 0 or previous_week.sales > 0
            then round(((current_week.sales-previous_week.sales)/(previous_week.sales))*100)
        end as WOW_sales_percentage,
		


	current_week.sales as current_week,
    previous_year_week.sales as previous_year_week,
    case
        when current_week.sales < 0 or previous_year_week.sales <= 0
            then NULL
        when current_week.sales >= 0 or previous_year_week.sales > 0
            then round(((current_week.sales-previous_year_week.sales)/(previous_year_week.sales))*100)
        end as YoY_sales_percentage,


    current_ytd_week.sales as YTD_2022,
    previous_ytd_week.sales as YTD_2021,
    case
        when current_ytd_week.sales < 0 or previous_ytd_week.sales <= 0
            then NULL
        when current_ytd_week.sales >= 0 or previous_ytd_week.sales > 0
            then round(((current_ytd_week.sales-previous_ytd_week.sales)/(previous_ytd_week.sales))*100)
        end as YoY_sales_percentage


from date
full join current_week on date.store_number = current_week.store_number
full join previous_week on date.store_number = previous_week.store_number
full join previous_year_week on date. store_number = previous_year_week.store_number
full join current_ytd_week on date.store_number = current_ytd_week.store_number
full join previous_ytd_week on date.store_number = previous_ytd_week.store_number