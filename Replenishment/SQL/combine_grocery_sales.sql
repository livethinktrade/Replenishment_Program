/*used to collect all of the data from the different schemas for all of the stores and combine them  */

with groccery_sales as (


        select acme.sales2.*
        from acme.sales2
        inner join public.item_support2 on acme.sales2.code = public.item_support2.code

        union all

        select sal.sales2.*
        from sal.sales2
        inner join public.item_support2 on sal.sales2.code = public.item_support2.code
	
        union all

        select midwest.sales2.*
        from midwest.sales2
        inner join public.item_support2 on midwest.sales2.code = public.item_support2.code

        union all


        select intermountain.sales2.*
        from intermountain.sales2
        inner join public.item_support2 on intermountain.sales2.code = public.item_support2.code

        union all


        select jewel.sales2.*
        from jewel.sales2
        inner join public.item_support2 on jewel.sales2.code = public.item_support2.code

        union all


        select kroger_atlanta.sales2.*
        from kroger_atlanta.sales2
        inner join public.item_support2 on kroger_atlanta.sales2.code = public.item_support2.code

        union all


        select kroger_central.sales2.*
        from kroger_central.sales2
        inner join public.item_support2 on kroger_central.sales2.code = public.item_support2.code

        union all


        select kroger_cincinatti.sales2.*
        from kroger_cincinatti.sales2
        inner join public.item_support2 on kroger_cincinatti.sales2.code = public.item_support2.code

        union all


        select kroger_columbus.sales2.*
        from kroger_columbus.sales2
        inner join public.item_support2 on kroger_columbus.sales2.code = public.item_support2.code

        union all


        select kroger_dallas.sales2.*
        from kroger_dallas.sales2
        inner join public.item_support2 on kroger_dallas.sales2.code = public.item_support2.code

        union all


        select kroger_delta.sales2.*
        from kroger_delta.sales2
        inner join public.item_support2 on kroger_delta.sales2.code = public.item_support2.code

        union all


        select kroger_dillons.sales2.*
        from kroger_dillons.sales2
        inner join public.item_support2 on kroger_dillons.sales2.code = public.item_support2.code

        union all


        select kroger_king_soopers.sales2.*
        from kroger_king_soopers.sales2
        inner join public.item_support2 on kroger_king_soopers.sales2.code = public.item_support2.code

        union all


        select kroger_louisville.sales2.*
        from kroger_louisville.sales2
        inner join public.item_support2 on kroger_louisville.sales2.code = public.item_support2.code

        union all


        select kroger_michigan.sales2.*
        from kroger_michigan.sales2
        inner join public.item_support2 on kroger_michigan.sales2.code = public.item_support2.code

        union all


        select kroger_nashville.sales2.*
        from kroger_nashville.sales2
        inner join public.item_support2 on kroger_nashville.sales2.code = public.item_support2.code

        union all


        select kvat.sales2.*
        from kvat.sales2
        inner join public.item_support2 on kvat.sales2.code = public.item_support2.code

        union all


        select safeway_denver.sales2.*
        from safeway_denver.sales2
        inner join public.item_support2 on safeway_denver.sales2.code = public.item_support2.code

        union all


        select texas_division.sales2.*
        from texas_division.sales2
        inner join public.item_support2 on texas_division.sales2.code = public.item_support2.code

        )

select distinct * from groccery_sales;
