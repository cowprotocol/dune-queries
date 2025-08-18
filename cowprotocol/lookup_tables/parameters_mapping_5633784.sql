select
    map_from_entries(
        array[
              ('last 7d'    , date_add('day', -7, current_date))
            , ('last 30d'   , date_add('day', -30, current_date))
            , ('last 3m'    , date_add('month', -3, date_trunc('month',current_date)))
            , ('last 6m'    , date_add('month', -6, date_trunc('month',current_date)))
            , ('last 12m'   , date_add('month', -12, date_trunc('month',current_date))) 
            , ('last 2y'    , date_add('month', -24, date_trunc('month',current_date))) 
            , ('last 3y'    , date_add('month', -36, date_trunc('month',current_date))) 
            , ('last 5y'    , date_add('month', -60, date_trunc('month',current_date))) 
            , ('all time'   , date('2010-01-01'))
        ]
    ) as start_date -- mapped in https://dune.com/queries/5633832 for list of parameters
