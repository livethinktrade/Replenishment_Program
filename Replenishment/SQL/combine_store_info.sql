with groccery_sales as (


        select acme.store.*
        from acme.store

        union all

        select sal.store.*
        from sal.store

	
        union all

        select midwest.store.*
        from midwest.store

        union all


        select intermountain.store.*
        from intermountain.store


        union all


        select jewel.store.*
        from jewel.store

        union all


        select kroger_atlanta.store.*
        from kroger_atlanta.store

        union all


        select kroger_central.store.*
        from kroger_central.store


        union all


        select kroger_cincinatti.store.*
        from kroger_cincinatti.store


        union all


        select kroger_columbus.store.*
        from kroger_columbus.store


        union all


        select kroger_dallas.store.*
        from kroger_dallas.store


        union all


        select kroger_delta.store.*
        from kroger_delta.store


        union all


        select kroger_dillons.store.*
        from kroger_dillons.store


        union all


        select kroger_king_soopers.store.*
        from kroger_king_soopers.store


        union all


        select kroger_louisville.store.*
        from kroger_louisville.store


        union all


        select kroger_michigan.store.*
        from kroger_michigan.store


        union all


        select kroger_nashville.store.*
        from kroger_nashville.store


        union all


        select kvat.store.*
        from kvat.store


        union all


        select safeway_denver.store.*
        from safeway_denver.store

        union all


        select texas_division.store.*
        from texas_division.store


        )

select distinct * from groccery_sales;
