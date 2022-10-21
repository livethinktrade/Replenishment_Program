with groccery_sales as (


        select acme.store_program_history.*
        from acme.store_program_history

        union all

        select sal.store_program_history.*
        from sal.store_program_history

        union all

        select midwest.store_program_history.*
        from midwest.store_program_history

        union all


        select intermountain.store_program_history.*
        from intermountain.store_program_history

        union all


        select jewel.store_program_history.*
        from jewel.store_program_history

        union all


        select kroger_atlanta.store_program_history.*
        from kroger_atlanta.store_program_history

        union all


        select kroger_central.store_program_history.*
        from kroger_central.store_program_history

        union all


        select kroger_cincinatti.store_program_history.*
        from kroger_cincinatti.store_program_history

        union all


        select kroger_columbus.store_program_history.*
        from kroger_columbus.store_program_history

        union all


        select kroger_dallas.store_program_history.*
        from kroger_dallas.store_program_history

        union all


        select kroger_delta.store_program_history.*
        from kroger_delta.store_program_history

        union all


        select kroger_dillons.store_program_history.*
        from kroger_dillons.store_program_history

        union all


        select kroger_king_soopers.store_program_history.*
        from kroger_king_soopers.store_program_history

        union all


        select kroger_louisville.store_program_history.*
        from kroger_louisville.store_program_history

        union all


        select kroger_michigan.store_program_history.*
        from kroger_michigan.store_program_history

        union all


        select kroger_nashville.store_program_history.*
        from kroger_nashville.store_program_history

        union all


        select kvat.store_program_history.*
        from kvat.store_program_history

        union all


        select safeway_denver.store_program_history.*
        from safeway_denver.store_program_history

        union all


        select texas_division.store_program_history.*
        from texas_division.store_program_history

        )

select distinct * from groccery_sales;
