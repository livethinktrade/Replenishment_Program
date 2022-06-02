
 /*create view sales_table as (
 SELECT DISTINCT sales2.id,
    sales2.transition_year,
    sales2.transition_season,
    sales2.store_year,
    sales2.store_week,
    sales2.store_number,
    sales2.upc AS upc_11_digit,
    sales2.sales,
    sales2.qty,
    sales2.current_year,
    sales2.store_type,
    item_support2.season,
    item_support2.category,
    item_support2.upc,
    item_support2.display_size,
    item_support2.case_size,
	item_support2.item_group_desc
   FROM sales2
   inner join item_support2 on sales2.upc = item_support2.upc_11_digit);

create view delivery_table as (

	/*This view allows for
	select distinct(id), transition_year, transition_season, delivery2.type, date, delivery2.upc, store,
	qty, season, category, item_group_desc, display_size, case_size
	from delivery2
	inner join item_support2 on delivery2.upc = item_support2.upc); */

/*new code FOR sd combo*/

with

	delivery_table as (

		/*This table is basically the delivery table with the support sheet lined up */
		select distinct(id), transition_year, transition_season, delivery2.type, date, delivery2.upc, store,
		qty, season, category, item_group_desc, display_size, case_size
		from delivery2
		inner join item_support2 on delivery2.code = item_support2.code),

    deliv_pivot_credit as (

		/*this next table will grab all of credits for ay items along with credits for whatever season we are in*/

        SELECT store,
			item_group_desc,
            sum(qty) AS credit,
            display_size,
            season,
            case_size


        FROM delivery_table

		where
				/*this next line tracks AY items will need to use python to dynamically track 2 years worth only*/
			(

				(
					season = 'AY'
				 	and type = 'Credit Memo'
				 	and (category != 'Accessory' and category != 'GM' and category != 'Scrub')
				)

				 or

				/*this next line should be dynamic using python keep in mind that rolling transition will include SS and FW items */
				(
					season = 'SS'
				 	and type = 'Credit Memo'
				 	and (category != 'Accessory' and category != 'GM' and category != 'Scrub')
				 	and transition_year = 2022
				 	and transition_season = 'SS'
				)

			  )

		group by store, item_group_desc,display_size,season, case_size
		order by store),

	deliv_pivot_invoice as (


		SELECT store,
			item_group_desc,
			sum(qty) AS deliveries,
			display_size,
            season,
            case_size

		FROM delivery_table

		where
				/*this next line tracks AY items will need to use python to dynamically track 2 years worth only*/
			(

				(
					season = 'AY'
				 	and (type = 'Invoice' or type = 'BandAid' or type = 'Reset')
				 	and (category != 'Accessory' and category != 'GM' and category != 'Scrub')
				)

				 or

				/*this next line should be dynamic using python keep in mind that rolling transition will include SS and FW items */
				(
					season = 'SS'
				 	and (type = 'Invoice' or type = 'BandAid' or type = 'Reset')
				 	and (category != 'Accessory' and category != 'GM' and category != 'Scrub')
				 	and transition_year = 2022
				 	and transition_season = 'SS'
				)

			  )

		group by store, item_group_desc,display_size,season, case_size
		order by store),

	delivery_pivot as (

		select deliv_pivot_invoice.store,

				deliv_pivot_invoice.item_group_desc,
				deliveries,
				COALESCE(credit,0) as credit,
				deliv_pivot_invoice.display_size,
                deliv_pivot_invoice.season,
                deliv_pivot_invoice.case_size
		from deliv_pivot_invoice
		full join deliv_pivot_credit on deliv_pivot_invoice.item_group_desc = deliv_pivot_credit.item_group_desc AND
					deliv_pivot_invoice.store = deliv_pivot_credit.store),

	sales_table as (

		SELECT DISTINCT sales2.id,
			sales2.transition_year,
			sales2.transition_season,
			sales2.store_year,
			sales2.store_week,
			sales2.store_number,
			sales2.upc AS upc_11_digit,
			sales2.sales,
			sales2.qty,
			sales2.current_year,
			sales2.store_type,
			item_support2.season,
			item_support2.category,
			item_support2.upc,
			item_support2.display_size,
			item_support2.case_size,
			item_support2.item_group_desc
		FROM sales2
		inner JOIN item_support2 ON sales2.upc = item_support2.upc_11_digit),

	sales_pivot_AY as (

		SELECT store_number,
				item_group_desc,
				sum(qty) AS sales

		from sales_table
		/* python dynamically */
		where season = 'AY' and (category != 'Accessory' and category != 'GM')
		GROUP BY store_number,item_group_desc
		ORDER BY store_number, item_group_desc),

	sales_pivot_FW as (
		SELECT store_number,
				item_group_desc,
				sum(qty) AS sales
		FROM sales_table

        /* python dynamically */

		where season = 'FW' and (category != 'Accessory' and category != 'GM') and transition_year = 2021 and transition_season = 'FW'
		GROUP BY store_number, item_group_desc
		ORDER BY store_number),

	sales_pivot_SS as (

		SELECT store_number,
				item_group_desc,
				sum(qty) AS sales
		FROM sales_table

        /* python dynamically */

		where season = 'SS' and (category != 'Accessory' and category != 'GM') and transition_year = 2022 and transition_season = 'SS'
		GROUP BY store_number, item_group_desc
		ORDER BY store_number),

    combine_sd as (

		/*combines all of the sales tables and then joins it to the delivery  table*/

		select delivery_pivot.store,
			   delivery_pivot.item_group_desc,
			   delivery_pivot.season,
			   delivery_pivot.display_size,
			   delivery_pivot.case_size,
			   deliveries,
			   credit,
			   sales_pivot_AY.item_group_desc as AY_Desc,
			   sales_pivot_FW.item_group_desc as FW_Desc,
			   sales_pivot_SS.item_group_desc as SS_Desc

		from delivery_pivot
		full join sales_pivot_fw on sales_pivot_fw.store_number = delivery_pivot.store and
				   sales_pivot_fw.item_group_desc = delivery_pivot.item_group_desc
		full join sales_pivot_ay on sales_pivot_ay.store_number = delivery_pivot.store and
				   sales_pivot_ay.item_group_desc = delivery_pivot.item_group_desc
		full join sales_pivot_ss on sales_pivot_ss.store_number = delivery_pivot.store and
				   sales_pivot_ss.item_group_desc = delivery_pivot.item_group_desc

		group by
				delivery_pivot.store,
			   delivery_pivot.item_group_desc,
			   delivery_pivot.season,
			   delivery_pivot.display_size,
			   delivery_pivot.case_size,
			   deliveries,
			   credit,
			   sales_pivot_AY.item_group_desc,
			   sales_pivot_FW.item_group_desc,
			   sales_pivot_SS.item_group_desc


		order by delivery_pivot.store asc, item_group_desc asc)


select
	   store,
	   combine_sd.item_group_desc,
	   display_size,
	   season,
	   case_size,
	   deliveries,
	   credit,
	   coalesce(sales_pivot_ss.sales,0) + coalesce(sales_pivot_fw.sales,0) + coalesce(sales_pivot_ay.sales,0) as total_sales,
	   (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0)) as on_hand,
	   case
	   		when (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0)) <= 0
			   	then 0
			when (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0)) > 0
				then (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0))/case_size
	      	end as case_qty

from combine_sd
full join sales_pivot_fw on sales_pivot_fw.store_number = combine_sd.store and
		   sales_pivot_fw.item_group_desc = combine_sd.item_group_desc
full join sales_pivot_ay on sales_pivot_ay.store_number = combine_sd.store and
		   sales_pivot_ay.item_group_desc = combine_sd.item_group_desc
full join sales_pivot_ss on sales_pivot_ss.store_number = combine_sd.store and
		   sales_pivot_ss.item_group_desc = combine_sd.item_group_desc

order by store asc, item_group_desc asc
