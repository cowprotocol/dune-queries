with all_data as (
    select
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_ethereum
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_ethereum
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_gnosis
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_gnosis
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_arbitrum
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_arbitrum
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_base
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_base
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_avalanche_c
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_avalanche_C
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_polygon
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_polygon
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_bnb
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_bnb
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_linea
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_linea
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_plasma
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_plasma
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_prod_data_per_solver_and_accounting_period_ink
    union all
    select 
        blockchain,
        env as environment,
        solver,
        accounting_period,
        cast(accounting_period_start_time as timestamp) as accounting_period_start_time,
        cast(accounting_period_end_time as timestamp) as accounting_period_end_time,
        cast(consistency_reward_native as decimal(38, 0)) as consistency_reward_native,
        cast(consistency_reward_cow as decimal(38, 0)) as consistency_reward_cow
    from dune.cowprotocol.dataset_staging_data_per_solver_and_accounting_period_ink
)

select * from all_data
where
    blockchain='{{blockchain}}'
    and
    accounting_period_start_time = cast('{{start_time}}' as timestamp)
    and
    accounting_period_end_time = cast('{{end_time}}' as timestamp)
