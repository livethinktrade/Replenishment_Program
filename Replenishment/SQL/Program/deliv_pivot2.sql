create view delivery_pivot as
with 
    deliv_pivot_credit as (
        SELECT delivery.store,
            item_support.item_group_desc,
            sum(delivery.qty) AS credit

        FROM delivery
        JOIN item_support ON delivery.upc = item_support.upc
        WHERE (delivery.type = 'Credit Memo' and item_support.gm = 'AY') or 
        /*season is statically typed in the future include f-string */
              (delivery.type = 'Credit Memo' and item_support.gm = 'SS' and	transition_date_range = '2022SS')

        GROUP BY delivery.store, item_support.item_group_desc
        ORDER BY delivery.store),

	deliv_pivot_invoice as ( 
        SELECT delivery.store,
            item_support.item_group_desc,
			sum(delivery.qty) AS deliveries
			
		FROM delivery
		JOIN item_support ON delivery.upc = item_support.upc
		WHERE ((delivery.type = 'Invoice' or delivery.type = 'BandAid' or delivery.type = 'Reset') and item_support.gm = 'AY') or
        /*season is statically typed in the future include f-string */
              ((delivery.type = 'Invoice' or delivery.type = 'BandAid' or delivery.type = 'Reset') and item_support.gm = 'SS' and transition_date_range = '2022SS')

		GROUP BY delivery.store,item_support.item_group_desc
		ORDER BY delivery.store),

   

	/* combines the invoice and credit tables for AY items */

	delivery_pivot as (
		select deliv_pivot_invoice.store, 

				deliv_pivot_invoice.item_group_desc, 
				deliveries, 
				COALESCE(credit,0) as credit
		from deliv_pivot_invoice
		full join deliv_pivot_credit on deliv_pivot_invoice.item_group_desc = deliv_pivot_credit.item_group_desc AND 
					deliv_pivot_invoice.store = deliv_pivot_credit.store)



select * from delivery_pivot;
