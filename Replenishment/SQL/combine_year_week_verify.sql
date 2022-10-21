with groccery_sales as (


        select acme.year_week_verify.*
        from acme.year_week_verify
   
        union all


        select intermountain.year_week_verify.*
        from intermountain.year_week_verify

        union all

        select texas_division.year_week_verify.*
        from texas_division.year_week_verify
        
        union all
        
        select follett.year_week_verify.*
        from follett.year_week_verify

        )

select distinct * from groccery_sales;
