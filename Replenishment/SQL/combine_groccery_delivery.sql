/*used to collect all of the data from the different schemas for all of the stores and combine them  */

with groccery_sales as (


        select acme.delivery2.*
        from acme.delivery2
        inner join public.item_support2 on acme.delivery2.code = public.item_support2.code

        union all

        select midwest.delivery2.*
        from midwest.delivery2
        inner join public.item_support2 on midwest.delivery2.code = public.item_support2.code

        union all

        select sal.delivery2.*
        from sal.delivery2
        inner join public.item_support2 on sal.delivery2.code = public.item_support2.code

        union all


        select intermountain.delivery2.*
        from intermountain.delivery2
        inner join public.item_support2 on intermountain.delivery2.code = public.item_support2.code

        union all


        select jewel.delivery2.*
        from jewel.delivery2
        inner join public.item_support2 on jewel.delivery2.code = public.item_support2.code

        union all


        select kroger_atlanta.delivery2.*
        from kroger_atlanta.delivery2
        inner join public.item_support2 on kroger_atlanta.delivery2.code = public.item_support2.code

        union all


        select kroger_central.delivery2.*
        from kroger_central.delivery2
        inner join public.item_support2 on kroger_central.delivery2.code = public.item_support2.code

        union all


        select kroger_cincinatti.delivery2.*
        from kroger_cincinatti.delivery2
        inner join public.item_support2 on kroger_cincinatti.delivery2.code = public.item_support2.code

        union all


        select kroger_columbus.delivery2.*
        from kroger_columbus.delivery2
        inner join public.item_support2 on kroger_columbus.delivery2.code = public.item_support2.code

        union all


        select kroger_dallas.delivery2.*
        from kroger_dallas.delivery2
        inner join public.item_support2 on kroger_dallas.delivery2.code = public.item_support2.code

        union all


        select kroger_delta.delivery2.*
        from kroger_delta.delivery2
        inner join public.item_support2 on kroger_delta.delivery2.code = public.item_support2.code

        union all


        select kroger_dillons.delivery2.*
        from kroger_dillons.delivery2
        inner join public.item_support2 on kroger_dillons.delivery2.code = public.item_support2.code

        union all


        select kroger_king_soopers.delivery2.*
        from kroger_king_soopers.delivery2
        inner join public.item_support2 on kroger_king_soopers.delivery2.code = public.item_support2.code

        union all


        select kroger_louisville.delivery2.*
        from kroger_louisville.delivery2
        inner join public.item_support2 on kroger_louisville.delivery2.code = public.item_support2.code

        union all


        select kroger_michigan.delivery2.*
        from kroger_michigan.delivery2
        inner join public.item_support2 on kroger_michigan.delivery2.code = public.item_support2.code

        union all


        select kroger_nashville.delivery2.*
        from kroger_nashville.delivery2
        inner join public.item_support2 on kroger_nashville.delivery2.code = public.item_support2.code

        union all


        select kvat.delivery2.*
        from kvat.delivery2
        inner join public.item_support2 on kvat.delivery2.code = public.item_support2.code

        union all


        select safeway_denver.delivery2.*
        from safeway_denver.delivery2
        inner join public.item_support2 on safeway_denver.delivery2.code = public.item_support2.code

        union all


        select texas_division.delivery2.*
        from texas_division.delivery2
        inner join public.item_support2 on texas_division.delivery2.code = public.item_support2.code

        )

select distinct * from groccery_sales;


