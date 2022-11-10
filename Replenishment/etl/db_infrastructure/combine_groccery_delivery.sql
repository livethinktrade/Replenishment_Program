/*used to collect all of the data from the different schemas for all of the stores and combine them  */

with groccery_sales as (


        select acme.delivery2.*, unique_replen_code
        from acme.delivery2
        inner join public.item_support2 on acme.delivery2.code = public.item_support2.code

        union all


        select follett.delivery2.*, unique_replen_code
        from follett.delivery2
        inner join public.item_support2 on follett.delivery2.code = public.item_support2.code

        union all

        select fresh_encounter.delivery2.*, unique_replen_code
        from fresh_encounter.delivery2
        inner join public.item_support2 on fresh_encounter.delivery2.code = public.item_support2.code

        union all


        select intermountain.delivery2.*, unique_replen_code
        from intermountain.delivery2
        inner join public.item_support2 on intermountain.delivery2.code = public.item_support2.code

        union all


        select jewel.delivery2.*, unique_replen_code
        from jewel.delivery2
        inner join public.item_support2 on jewel.delivery2.code = public.item_support2.code

        union all


        select kroger_atlanta.delivery2.*, unique_replen_code
        from kroger_atlanta.delivery2
        inner join public.item_support2 on kroger_atlanta.delivery2.code = public.item_support2.code

        union all


        select kroger_central.delivery2.*, unique_replen_code
        from kroger_central.delivery2
        inner join public.item_support2 on kroger_central.delivery2.code = public.item_support2.code

        union all


        select kroger_cincinatti.delivery2.*, unique_replen_code
        from kroger_cincinatti.delivery2
        inner join public.item_support2 on kroger_cincinatti.delivery2.code = public.item_support2.code

        union all


        select kroger_columbus.delivery2.*, unique_replen_code
        from kroger_columbus.delivery2
        inner join public.item_support2 on kroger_columbus.delivery2.code = public.item_support2.code

        union all


        select kroger_dallas.delivery2.*, unique_replen_code
        from kroger_dallas.delivery2
        inner join public.item_support2 on kroger_dallas.delivery2.code = public.item_support2.code

        union all


        select kroger_delta.delivery2.*, unique_replen_code
        from kroger_delta.delivery2
        inner join public.item_support2 on kroger_delta.delivery2.code = public.item_support2.code

        union all


        select kroger_dillons.delivery2.*, unique_replen_code
        from kroger_dillons.delivery2
        inner join public.item_support2 on kroger_dillons.delivery2.code = public.item_support2.code

        union all


        select kroger_king_soopers.delivery2.*, unique_replen_code
        from kroger_king_soopers.delivery2
        inner join public.item_support2 on kroger_king_soopers.delivery2.code = public.item_support2.code

        union all


        select kroger_louisville.delivery2.*, unique_replen_code
        from kroger_louisville.delivery2
        inner join public.item_support2 on kroger_louisville.delivery2.code = public.item_support2.code

        union all


        select kroger_michigan.delivery2.*, unique_replen_code
        from kroger_michigan.delivery2
        inner join public.item_support2 on kroger_michigan.delivery2.code = public.item_support2.code

        union all


        select kroger_nashville.delivery2.*, unique_replen_code
        from kroger_nashville.delivery2
        inner join public.item_support2 on kroger_nashville.delivery2.code = public.item_support2.code

        union all


        select kvat.delivery2.*, unique_replen_code
        from kvat.delivery2
        inner join public.item_support2 on kvat.delivery2.code = public.item_support2.code

        union all


        select safeway_denver.delivery2.*, unique_replen_code
        from safeway_denver.delivery2
        inner join public.item_support2 on safeway_denver.delivery2.code = public.item_support2.code

        union all


        select texas_division.delivery2.*, unique_replen_code
        from texas_division.delivery2
        inner join public.item_support2 on texas_division.delivery2.code = public.item_support2.code

        )

select distinct * from groccery_sales;


