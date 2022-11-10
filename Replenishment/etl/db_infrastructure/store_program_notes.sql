/*used to collect all of the data from the different schemas for all of the stores and combine them  */

with groccery_sales as (


        select acme.store.store_id, program_id, activity, notes, acme.store.store_type
        from acme.store_program
        inner join acme.store on acme.store_program.store_id = acme.store.store_id

        union all


        select follett.store.store_id, program_id, activity, notes, follett.store.store_type
        from follett.store_program
        inner join follett.store on follett.store_program.store_id = follett.store.store_id

        union all

        select fresh_encounter.store.store_id, program_id, activity, notes, fresh_encounter.store.store_type
        from fresh_encounter.store_program
        inner join fresh_encounter.store on fresh_encounter.store_program.store_id = fresh_encounter.store.store_id

        union all


        select intermountain.store.store_id, program_id, activity, notes, intermountain.store.store_type
        from intermountain.store_program
        inner join intermountain.store on intermountain.store_program.store_id = intermountain.store.store_id

        union all


        select jewel.store.store_id, program_id, activity, notes, jewel.store.store_type
        from jewel.store_program
        inner join jewel.store on jewel.store_program.store_id = jewel.store.store_id

        union all


        select kroger_atlanta.store.store_id, program_id, activity, notes, kroger_atlanta.store.store_type
        from kroger_atlanta.store_program
        inner join kroger_atlanta.store on kroger_atlanta.store_program.store_id = kroger_atlanta.store.store_id

        union all


        select kroger_central.store.store_id, program_id, activity, notes, kroger_central.store.store_type
        from kroger_central.store_program
        inner join kroger_central.store on kroger_central.store_program.store_id = kroger_central.store.store_id

        union all


        select kroger_cincinatti.store.store_id, program_id, activity, notes, kroger_cincinatti.store.store_type
        from kroger_cincinatti.store_program
        inner join kroger_cincinatti.store on kroger_cincinatti.store_program.store_id = kroger_cincinatti.store.store_id

        union all


        select kroger_columbus.store.store_id, program_id, activity, notes, kroger_columbus.store.store_type
        from kroger_columbus.store_program
        inner join kroger_columbus.store on kroger_columbus.store_program.store_id = kroger_columbus.store.store_id

        union all


        select kroger_dallas.store.store_id, program_id, activity, notes, kroger_dallas.store.store_type
        from kroger_dallas.store_program
        inner join kroger_dallas.store on kroger_dallas.store_program.store_id = kroger_dallas.store.store_id

        union all


        select kroger_delta.store.store_id, program_id, activity, notes, kroger_delta.store.store_type
        from kroger_delta.store_program
        inner join kroger_delta.store on kroger_delta.store_program.store_id = kroger_delta.store.store_id

        union all


        select kroger_dillons.store.store_id, program_id, activity, notes, kroger_dillons.store.store_type
        from kroger_dillons.store_program
        inner join kroger_dillons.store on kroger_dillons.store_program.store_id = kroger_dillons.store.store_id

        union all


        select kroger_king_soopers.store.store_id, program_id, activity, notes, kroger_king_soopers.store.store_type
        from kroger_king_soopers.store_program
        inner join kroger_king_soopers.store on kroger_king_soopers.store_program.store_id = kroger_king_soopers.store.store_id

        union all


        select kroger_louisville.store.store_id, program_id, activity, notes, kroger_louisville.store.store_type
        from kroger_louisville.store_program
        inner join kroger_louisville.store on kroger_louisville.store_program.store_id = kroger_louisville.store.store_id

        union all


        select kroger_michigan.store.store_id, program_id, activity, notes, kroger_michigan.store.store_type
        from kroger_michigan.store_program
        inner join kroger_michigan.store on kroger_michigan.store_program.store_id = kroger_michigan.store.store_id

        union all


        select kroger_nashville.store.store_id, program_id, activity, notes, kroger_nashville.store.store_type
        from kroger_nashville.store_program
        inner join kroger_nashville.store on kroger_nashville.store_program.store_id = kroger_nashville.store.store_id

        union all


        select kvat.store.store_id, program_id, activity, notes, kvat.store.store_type
        from kvat.store_program
        inner join kvat.store on kvat.store_program.store_id = kvat.store.store_id

        union all


        select safeway_denver.store.store_id, program_id, activity, notes, safeway_denver.store.store_type
        from safeway_denver.store_program
        inner join safeway_denver.store on safeway_denver.store_program.store_id = safeway_denver.store.store_id

        union all


        select texas_division.store.store_id, program_id, activity, notes, texas_division.store.store_type
        from texas_division.store_program
        inner join texas_division.store on texas_division.store_program.store_id = texas_division.store.store_id

        )

select distinct * from groccery_sales;


