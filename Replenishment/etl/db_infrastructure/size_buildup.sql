 WITH 
 		sales_by_size AS (
         
	 
			 /*selects all of the large items in the sales */
				SELECT sales.store_number AS store,
					item_support.season,
					item_support.item_group_desc,
					item_support.size,
					item_support.display_size,
					sum(sales.qty) AS unit_sales
				 FROM sales
				 JOIN item_support ON sales.upc::text = item_support.upc_11_digit::text
				 WHERE item_support.size::text = 'L'::text AND (item_support.season = 'AY'::bpchar)
				 GROUP BY   sales.store_number, item_support.item_group_desc, item_support.season, item_support.display_size, item_support.size
				 order by store),
		 
		 delivery_by_size as (
				SELECT store,
						item_support.season,
						item_support.item_group_desc,
						item_support.size,
						item_support.display_size,    
						sum(qty) AS unit_deliv
				FROM delivery
				inner JOIN item_support ON item_support.upc = delivery.upc
				WHERE item_support.size = 'L' AND item_support.season = 'AY'
				GROUP BY  store, item_support.season, item_support.item_group_desc, item_support.size, item_support.display_size
				order by store),
				
		combo as (
				
				select delivery_by_size.store, delivery_by_size.item_group_desc, unit_deliv, unit_sales, (unit_deliv-unit_sales) as on_hand
				from delivery_by_size
				inner join sales_by_size on delivery_by_size.store = sales_by_size.store and 
							delivery_by_size.item_group_desc = sales_by_size.item_group_desc), 
							
		large_case_qty as (
		
				select store, display_size, 
						combo.item_group_desc, 
						max(item_support.total_case_size) as total_case_size, 
						on_hand, 
						(on_hand/(max(item_support.total_case_size))) as case_qty
				from combo
				inner join item_support on item_support.item_group_desc = combo.item_group_desc
				group by store, on_hand, combo.item_group_desc, display_size),
				
		size_calculations as (
				
				select large_case_qty.store, 
						item_group_desc, 
						display_size, 
						total_case_size, 
						on_hand as on_hand_large, 
						case_qty, 
						/* next line is only band aid approach does not take in account for long hangin if you want to use if statement to solve problem*/
						carded,
						round((case_qty/carded),2) as percentage_of_large_by_displaysize
				from large_case_qty 
				inner join case_capacity on large_case_qty.store = case_capacity.store),
				
				final_view as (
			
			select size_calculations.store, 
			size_calculations.item_group_desc, 
			on_hand_large,
			on_hand as store_total_on_hand,
			round((on_hand_large/on_hand),2) as percentage_of_large_by_item,
			percentage_of_large_by_displaysize,
			carded as total_space_for_display_size


			from size_calculations
			inner join sd_combo on sd_combo.item_group_desc = size_calculations.item_group_desc and 
					sd_combo.store = size_calculations.store
			group by
					size_calculations.store, 
					size_calculations.item_group_desc, 
					on_hand_large,
					on_hand,
					percentage_of_large_by_displaysize,
					carded
			having round((on_hand_large/on_hand),2) < 1.00 and round((on_hand_large/on_hand),2) > 0.00)
			

select * from final_view

		
		
				
		

				
		
