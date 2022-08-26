with

find_price as(
select qty, store_price, (qty * store_price) as dollar_shipped  from kroger_atlanta.delivery2
inner join kroger_atlanta.item_approval on kroger_atlanta.delivery2.code = kroger_atlanta.item_approval.code
where date > '1/29/2022' and kroger_atlanta.item_approval.store_price < 999)

select sum(dollar_shipped) from find_price