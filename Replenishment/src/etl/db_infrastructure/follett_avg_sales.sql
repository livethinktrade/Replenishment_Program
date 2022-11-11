with
    stores as (select distinct (follett.sales2.store_number) from follett.sales2 order by store_number),

    item as (select store_number,sum(sales) as sales, sum(qty) as qty, round((sales/qty),2) as average, item_group_desc
          from follett.sales2
          inner join "public".item_support2 on "public".item_support2.code = follett.sales2.code
          where (sales != 0 or qty !=0)
          group by store_number, item_group_desc, (sales/qty)
          order by store_number),

	item_sales as (select store_number, sum(sales) as sales, sum(qty) as qty, item_group_desc from item
				   group by store_number, item_group_desc),

    retail as (select  item_group_desc, max(store_price)*2 as "retail price"
                    from follett.item_approval
                    inner join "public".item_support2 on "public".item_support2.code = follett.item_approval.code
                    where store_price < 999
                    group by item_group_desc),

	retail_price as (select distinct(item_group_desc), "retail price" from retail)


select store_number, item_sales.item_group_desc, round((sales/qty),2) as "average sales", "retail price", round("retail price"-(sales/qty),2) as price_diff
from item_sales
left join retail_price on retail_price.item_group_desc = item_sales.item_group_desc
group by store_number, item_sales.item_group_desc, (sales/qty), "retail price"
having "retail price"-(sales/qty) >= 1
order by store_number
