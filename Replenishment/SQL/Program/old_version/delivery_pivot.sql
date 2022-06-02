create view delivery_pivot as
with deliv_pivot_credit as (
	SELECT delivery.store,
		item_support.mupc,
		sum(delivery.qty) AS credit

	FROM delivery
	JOIN item_support ON delivery.upc = item_support.upc
	WHERE delivery.type = 'Credit Memo' 
	GROUP BY delivery.store, item_support.mupc, item_support.item_group_desc, item_support.season, item_support.total_case_size
	ORDER BY delivery.store),

	deliv_pivot_invoice as (
		SELECT delivery.store,
			item_support.mupc,
			sum(delivery.qty) AS deliveries,
			item_support.item_group_desc,
			item_support.display_size,
			item_support.season,
			item_support.total_case_size
		FROM delivery
		JOIN item_support ON delivery.upc = item_support.upc
		WHERE delivery.type = 'Invoice' or delivery.type = 'BandAid' or delivery.type = 'Reset'
		GROUP BY delivery.store, item_support.mupc, item_support.item_group_desc, item_support.season, item_support.total_case_size, item_support.display_size
		ORDER BY delivery.store),
		
	/* combines the invoice and credit tables */

	delivery_pivot as (
		select deliv_pivot_invoice.store, 
				deliv_pivot_invoice.mupc, 
				item_group_desc, 
				display_size,
				season, 
				total_case_size, 
				deliveries, 
				COALESCE(credit,0) as credit
		from deliv_pivot_invoice
		full join deliv_pivot_credit on deliv_pivot_invoice.mupc = deliv_pivot_credit.mupc AND 
					deliv_pivot_invoice.store = deliv_pivot_credit.store)

select * from delivery_pivot;




