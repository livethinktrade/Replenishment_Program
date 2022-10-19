with groccery_sales as (


        select acme.item_approval.*
        from acme.item_approval

        union all

        select sal.item_approval.*
        from sal.item_approval


        union all

        select midwest.item_approval.*
        from midwest.item_approval

        union all


        select intermountain.item_approval.*
        from intermountain.item_approval


        union all


        select jewel.item_approval.*
        from jewel.item_approval

        union all


        select kroger_atlanta.item_approval.*
        from kroger_atlanta.item_approval

        union all


        select kroger_central.item_approval.*
        from kroger_central.item_approval


        union all


        select kroger_cincinatti.item_approval.*
        from kroger_cincinatti.item_approval


        union all


        select kroger_columbus.item_approval.*
        from kroger_columbus.item_approval


        union all


        select kroger_dallas.item_approval.*
        from kroger_dallas.item_approval


        union all


        select kroger_delta.item_approval.*
        from kroger_delta.item_approval


        union all


        select kroger_dillons.item_approval.*
        from kroger_dillons.item_approval


        union all


        select kroger_king_soopers.item_approval.*
        from kroger_king_soopers.item_approval


        union all


        select kroger_louisville.item_approval.*
        from kroger_louisville.item_approval


        union all


        select kroger_michigan.item_approval.*
        from kroger_michigan.item_approval


        union all


        select kroger_nashville.item_approval.*
        from kroger_nashville.item_approval


        union all


        select kvat.item_approval.*
        from kvat.item_approval


        union all


        select safeway_denver.item_approval.*
        from safeway_denver.item_approval

        union all


        select texas_division.item_approval.*
        from texas_division.item_approval


        )

select * from groccery_sales;
