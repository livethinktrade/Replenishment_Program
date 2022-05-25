create view replenishment as
/*calculates the available space  by taking the alocated program case capacity minus the on hand */
with colapse as(
	select  sd_combo.store, 
			display_size, 
			sum(case_qty) as case_qty,
			carded,
			long_hanging_top,
			long_hanging_dress,
			case
				when display_size = 'Carded' then (carded - sum(case_qty))
				when display_size = 'Long Hanging Top' then (long_hanging_top - sum(case_qty))
				when display_size = 'Long Hanging Dress' then (long_hanging_dress - sum(case_qty))
				end as available_space

	from sd_combo
	inner join case_capacity on sd_combo.store = case_capacity.store
	
	/*THIS LINE IS DEPENDENT ON THE SEASON WE ARE CURRENTLY IN, IF FW THEN SWITCH SS TO FW*/
	where season = 'AY' or season = 'SS'
	
	group by sd_combo.store ,display_size, long_hanging_top, long_hanging_dress, carded
	order by sd_combo.store asc),
	
			/*select only certain columns with previous query*/	
		colapse_2 as(
			select store, display_size, available_space from colapse),

			/*combines sd combo view with query you just made above  */
		case_qty as(


			select  sd_combo.store, 
					item_group_desc, 
					sd_combo.display_size, 
					sum(case_qty) as case_qty,
					case
						when sd_combo.display_size = colapse_2.display_size and sd_combo.store = colapse_2.store
							then round(colapse_2.available_space)
						end as case_space_available
			from sd_combo
			inner join colapse_2 on sd_combo.store = colapse_2.store
			
			/*THIS LINE IS DEPENDENT ON THE SEASON WE ARE CURRENTLY IN, IF FW THEN SWITCH SS TO FW*/
			where season = 'AY' or season = 'SS'
			
			group by sd_combo.store, item_group_desc, sd_combo.display_size, colapse_2.display_size, colapse_2.available_space, colapse_2.store, sd_combo.credit, sd_combo.deliveries
			
			/*filters out any item that has a return ratio higher than 50% note that this line of code is located here and not up there bc you want to include the case_qty towards
			the space available calculations */
			
			having (abs(credit)/(deliveries+.01)) <= 0.50
			order by sd_combo.store)
			


/*filters the query you just made above. takes out any case space available that is < 0 also case_qty that is less than whatever threshold set*/
			
select case_qty.store, case_qty.item_group_desc, case_qty.display_size, case_qty, case_space_available, notes, max(availability) as availability 
from case_qty
inner join case_capacity on case_capacity.store = case_qty.store
inner join item_support on case_qty.item_group_desc = item_support.item_group_desc
where 
	case_space_available > 0 and 
	case_qty <= 0.50 
	
group by case_qty.store, case_qty.item_group_desc, case_qty.display_size, case_qty, case_space_available, notes

/*Filters out any items that we dont have in stock*/
having max(availability) > 0

order by store, case_qty