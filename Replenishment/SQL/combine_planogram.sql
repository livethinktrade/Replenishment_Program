with groccery_sales as (


        select acme.store_program.*
        from acme.store_program

        union all

        select sal.store_program.*
        from sal.store_program


        union all

        select midwest.store_program.*
        from midwest.store_program

        union all


        select intermountain.store_program.*
        from intermountain.store_program


        union all


        select jewel.store_program.*
        from jewel.store_program

        union all


        select kroger_atlanta.store_program.*
        from kroger_atlanta.store_program

        union all


        select kroger_central.store_program.*
        from kroger_central.store_program


        union all


        select kroger_cincinatti.store_program.*
        from kroger_cincinatti.store_program


        union all


        select kroger_columbus.store_program.*
        from kroger_columbus.store_program


        union all


        select kroger_dallas.store_program.*
        from kroger_dallas.store_program


        union all


        select kroger_delta.store_program.*
        from kroger_delta.store_program


        union all


        select kroger_dillons.store_program.*
        from kroger_dillons.store_program


        union all


        select kroger_king_soopers.store_program.*
        from kroger_king_soopers.store_program


        union all


        select kroger_louisville.store_program.*
        from kroger_louisville.store_program


        union all


        select kroger_michigan.store_program.*
        from kroger_michigan.store_program


        union all


        select kroger_nashville.store_program.*
        from kroger_nashville.store_program


        union all


        select kvat.store_program.*
        from kvat.store_program


        union all


        select safeway_denver.store_program.*
        from safeway_denver.store_program

        union all


        select texas_division.store_program.*
        from texas_division.store_program


        )

select * from groccery_sales;
