/* NOTE THAT THIS IS VERSION TWO OF THE sales deliv combo TABLE THIS VERSION IS
DIFFERENT DUE TO THE FACT THAT THE CODE IS GROUPING BY 'GROUP ITEM DESCRIPTION INSTEAD OF MUPC.
BY DOING THIS, THIS TAKES INTO ACCOUNT POSSIBLE CHANGES IN MUPC*/



/* sales pivot table view for all year */
create view sd_combo as
with sales_pivot_AY as (
            SELECT sales.store_number,
                    item_support.item_group_desc,
                    sum(sales.qty) AS sales
            FROM sales
            inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
            where item_support.gm = 'AY' 
            GROUP BY sales.store_number, item_support.item_group_desc
            ORDER BY sales.store_number, item_support.item_group_desc
			),

        /* this will select all of the fall winter sales data that was made during the current season prior to transition */
     sales_pivot_FW as (
            SELECT sales.store_number,
                    item_support.item_group_desc,
                    sum(sales.qty) AS sales
            FROM sales
            inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
            where transition_date_range = '2021FW' and item_support.gm = 'FW'
            GROUP BY sales.store_number, item_support.item_group_desc
            ORDER BY sales.store_number),


        /* this will select all of the Spring-summer sales data that was made during the current season  */
     sales_pivot_SS as (
            SELECT sales.store_number,
                    item_support.item_group_desc,
                    sum(sales.qty) AS sales
            FROM sales
            inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
            where transition_date_range = '2022SS' and item_support.gm = 'SS'
            GROUP BY sales.store_number, item_support.item_group_desc
            ORDER BY sales.store_number),
			
	combine_sd as (

			/*combines all of the sales tables and then joins it to the delivery  table*/

			select delivery_pivot.store,
				   delivery_pivot.item_group_desc,
				   item_support.display_size,
				   item_support.season,
				   /*max is set here bc total case size  has diffent total case size for different mupcs*/
				   max(item_support.total_case_size) as total_case_size,
				   deliveries,
				   credit,
				   sales_pivot_AY.item_group_desc as AY_Desc,
				   sales_pivot_FW.item_group_desc as FW_Desc,
				   sales_pivot_SS.item_group_desc as SS_Desc

			from delivery_pivot
			join item_support on delivery_pivot.item_group_desc = item_support.item_group_desc
			full join sales_pivot_fw on sales_pivot_fw.store_number = delivery_pivot.store and
					   sales_pivot_fw.item_group_desc = delivery_pivot.item_group_desc
			full join sales_pivot_ay on sales_pivot_ay.store_number = delivery_pivot.store and
					   sales_pivot_ay.item_group_desc = delivery_pivot.item_group_desc
			full join sales_pivot_ss on sales_pivot_ss.store_number = delivery_pivot.store and
					   sales_pivot_ss.item_group_desc = delivery_pivot.item_group_desc

			group by
					delivery_pivot.store,
				   delivery_pivot.item_group_desc,
				   item_support.display_size,
				   item_support.season,
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
	   total_case_size,
	   deliveries,
	   credit,
	   coalesce(sales_pivot_ss.sales,0) + coalesce(sales_pivot_fw.sales,0) + coalesce(sales_pivot_ay.sales,0) as total_sales,
	   (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0)) as on_hand,
	   case
	   		when (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0)) <= 0
			   	then 0
			when (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0)) > 0
				then (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0))/total_case_size
	      	end as case_qty

from combine_sd
full join sales_pivot_fw on sales_pivot_fw.store_number = combine_sd.store and
		   sales_pivot_fw.item_group_desc = combine_sd.item_group_desc
full join sales_pivot_ay on sales_pivot_ay.store_number = combine_sd.store and
		   sales_pivot_ay.item_group_desc = combine_sd.item_group_desc
full join sales_pivot_ss on sales_pivot_ss.store_number = combine_sd.store and
		   sales_pivot_ss.item_group_desc = combine_sd.item_group_desc
		   
order by store asc, item_group_desc asc

