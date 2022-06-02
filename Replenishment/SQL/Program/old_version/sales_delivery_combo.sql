/* sales pivot table view for all year */
create view sd_combo as
with sales_pivot_AY as (
            SELECT sales.store_number,
					sales.upc,
                    item_support.mupc,
                    sum(sales.qty) AS sales
            FROM sales
            inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
            where season = 'AY'
            GROUP BY sales.store_number, item_support.mupc, sales.upc
            ORDER BY sales.store_number),

        /* this will select all of the fall winter sales data that was made during the current season prior to transition */
     sales_pivot_FW as (
            SELECT sales.store_number,
		 			sales.upc,
                    item_support.mupc,
                    sum(sales.qty) AS sales
            FROM sales
            inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
            where transition_date_range = '2021FW' and season = 'FW'
            GROUP BY sales.store_number, item_support.mupc, sales.upc
            ORDER BY sales.store_number),


        /* this will select all of the Spring-summer sales data that was made during the current season  */
     sales_pivot_SS as (
            SELECT sales.store_number,
                    item_support.mupc,
		 			sales.upc,
                    sum(sales.qty) AS sales
            FROM sales
            inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
            where transition_date_range = '2022SS' and season = 'SS'
            GROUP BY sales.store_number, item_support.mupc, sales.upc
            ORDER BY sales.store_number)

/*combines all of the sales tables and then joins it to the delivery  table*/

select delivery_pivot.store,
	   delivery_pivot.mupc,
	   item_group_desc,
	   display_size,
	   season,
	   total_case_size,
	   deliveries,
	   credit,
	   sales_pivot_AY.mupc,
	   sales_pivot_FW.mupc,
	   sales_pivot_SS.mupc,
	   sales_pivot_AY.upc,
	   sales_pivot_FW.upc,
	   sales_pivot_SS.upc,
       /*the next line is combining the fw sales with the ay sales and naming total sales
       the next line is calculating on hand using the deliveries + credit - total sales
       next line is using
       
       */
	   coalesce(sales_pivot_ss.sales,0) + coalesce(sales_pivot_fw.sales,0) + coalesce(sales_pivot_ay.sales,0) as total_sales,
	   (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0)) as on_hand,
	   (deliveries + credit - coalesce(sales_pivot_fw.sales,0) - coalesce(sales_pivot_ay.sales,0)-coalesce(sales_pivot_ss.sales,0))/total_case_size as case_qty

from delivery_pivot

full join sales_pivot_fw on sales_pivot_fw.store_number = delivery_pivot.store and
		   sales_pivot_fw.mupc = delivery_pivot.mupc
full join sales_pivot_ay on sales_pivot_ay.store_number = delivery_pivot.store and
		   sales_pivot_ay.mupc = delivery_pivot.mupc
full join sales_pivot_ss on sales_pivot_ss.store_number = delivery_pivot.store and
		   sales_pivot_ss.mupc = delivery_pivot.mupc
 
order by delivery_pivot.store asc
