/* sales pivot table view for all year */
create view sales_pivot_AY as 
SELECT sales.store_number,
		item_support.mupc,
		sum(sales.qty) AS sales
FROM sales
inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
where season = 'AY'
GROUP BY sales.store_number, item_support.mupc
ORDER BY sales.store_number;




/* this will select all of the fall winter sales data that was made during the current season prior to transition */
create view sales_pivot_FW as 
SELECT sales.store_number,
		item_support.mupc,
		sum(sales.qty) AS sales
FROM sales
inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
where transition_date_range = '2021 FW' and season = 'FW'
GROUP BY sales.store_number, item_support.mupc
ORDER BY sales.store_number;


/* this will select all of the Spring-summer sales data that was made during the current season  */
create view sales_pivot_SS as 
SELECT sales.store_number,
		item_support.mupc,
		sum(sales.qty) AS sales
FROM sales
inner JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
where transition_date_range = '2022SS' and season = 'SS'
GROUP BY sales.store_number, item_support.mupc
ORDER BY sales.store_number;
