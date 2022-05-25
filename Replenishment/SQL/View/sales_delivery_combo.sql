create view sd_combo as
select delivery_pivot.store,
	   delivery_pivot.mupc,
	   item_desc,
	   display_size,
	   season,
	   total_case_size,
	   deliveries,
	   credit,
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
