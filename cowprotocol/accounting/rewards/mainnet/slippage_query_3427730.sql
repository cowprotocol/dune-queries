-- https://github.com/cowprotocol/solver-rewards/pull/342
with
block_range as (
    select * from "query_3333356(start_time='{{start_time}}',end_time='{{end_time}}')"
),

,final_token_balance_sheet as (
    select
        *
    from
        "query_4057345(start_time='{{start_time}}',end_time='{{end_time}}',tx_hash='{{tx_hash}}',solver_address='{{solver_address}}')"
)

token_times as (
    select
        hour,
        token
    from final_token_balance_sheet
    group by hour, token
),

precise_prices as (
    select
        contract_address,
        decimals,
        date_trunc('hour', minute) as hour,
        avg(
            case
                when (price > 10 and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab) then 0.26 -- dirty fix for some bogus COW prices Dune reports on July 29, 2024
                else price
            end
        ) as price
    from
        prices.usd
    inner join token_times
        on
            minute between date(hour) and date(hour) + interval '1' day -- query execution speed optimization since minute is indexed
            and date_trunc('hour', minute) = hour
            and contract_address = token
            and blockchain = 'ethereum'
    group by
        contract_address,
        decimals,
        date_trunc('hour', minute)
),

intrinsic_prices as (
    select
        contract_address,
        decimals,
        hour,
        avg(price) as price
    from (
        select
            buy_token_address as contract_address,
            round(log(10, atoms_bought / units_bought)) as decimals,
            date_trunc('hour', block_time) as hour,
            usd_value / units_bought as price
        from cow_protocol_ethereum.trades
        where
            block_number >= (select start_block from block_range) and block_number <= (select end_block from block_range)
            and units_bought > 0
        union distinct
        select
            sell_token_address as contract_address,
            round(log(10, atoms_sold / units_sold)) as decimals,
            date_trunc('hour', block_time) as hour,
            usd_value / units_sold as price
        from cow_protocol_ethereum.trades
        where
            block_number >= (select start_block from block_range) and block_number <= (select end_block from block_range)
            and units_sold > 0
    ) as combined
    group by hour, contract_address, decimals
    order by hour
),

-- -- Price Construction: https://dune.com/queries/1579091?
prices as (
    select
        tt.hour,
        tt.token as contract_address,
        coalesce(
            precise.decimals,
            intrinsic.decimals
        ) as decimals,
        coalesce(
            precise.price,
            intrinsic.price
        ) as price
    from token_times as tt
    left join precise_prices as precise
        on
            tt.hour = precise.hour
            and precise.contract_address = token
    left join intrinsic_prices as intrinsic
        on
            tt.hour = intrinsic.hour
            and intrinsic.contract_address = token
),

-- -- ETH Prices: https://dune.com/queries/1578626?d=1
eth_prices as (
    select
        date_trunc('hour', minute) as hour,
        avg(price) as eth_price
    from prices.usd
    where
        blockchain = 'ethereum'
        and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        and minute between cast('{{start_time}}' as timestamp) and cast('{{end_time}}' as timestamp)
    group by date_trunc('hour', minute)
),

results_per_tx as (
    select
        ftbs.hour,
        tx_hash,
        solver_address,
        sum(cast(token_imbalance_wei as double) * price / pow(10, p.decimals)) as usd_value,
        sum(cast(token_imbalance_wei as double) * price / pow(10, p.decimals) / eth_price) * pow(10, 18) as eth_slippage_wei,
        count(*) as num_entries
    from
        final_token_balance_sheet as ftbs
    left join prices as p
        on
            token = p.contract_address
            and ftbs.hour = p.hour
    left join eth_prices as ep
        on ftbs.hour = ep.hour
    group by
        ftbs.hour,
        solver_address,
        tx_hash
    having
        bool_and(price is not null)
),

results as (
    select
        solver_address,
        concat(environment, '-', name) as solver_name,
        sum(usd_value) as usd_value,
        sum(eth_slippage_wei) as eth_slippage_wei,
        concat(
            '<a href="https://dune.com/queries/3427730?SolverAddress=',
            cast(solver_address as varchar),
            '&CTE_NAME=results_per_tx',
            '&StartTime={{start_time}}',
            '&EndTime={{end_time}}',
            '" target="_blank">link</a>'
        ) as batchwise_breakdown
    from
        results_per_tx
    inner join cow_protocol_ethereum.solvers
        on address = solver_address
    group by
        solver_address,
        concat(environment, '-', name)
)

select * from {{cte_name}}
