with 
-- BEGIN VOUCH REGISTRY: https://dune.com/queries/2283344
bonding_pools (pool, name, initial_funder) as (
  select from_hex(pool), name, from_hex(funder) from (
    values {{BondingPoolData}}
  ) as _ (pool, name, funder)
),

last_block_before_timestamp as (
    select max(number) from ethereum.blocks
    where time < cast('{{EndTime}}' as timestamp)
),

-- Query Logic Begins here!
vouches as (
  select
    evt_block_number,
    evt_index,
    solver,
    cowRewardTarget as reward_target,
    pool,
    sender,
    True as active
  from cow_protocol_ethereum.VouchRegister_evt_Vouch
    join bonding_pools
        on pool = bondingPool
        and sender = initial_funder
  where evt_block_number <= (select * from last_block_before_timestamp)
),
invalidations as (
  select
    evt_block_number,
    evt_index,
    solver,
    Null as reward_target,  -- This is just ot align with vouches to take a union
    pool,
    sender,
    False as active
  from cow_protocol_ethereum.VouchRegister_evt_InvalidateVouch
    join bonding_pools
        on pool = bondingPool
        and sender = initial_funder
  where evt_block_number <= (select * from last_block_before_timestamp)
),
-- At this point we have excluded all arbitrary vouches (i.e. those not from initial funders of recognized pools)
-- This ranks (solver, pool, sender) by most recent (vouch or invalidation)
-- and yields as rank 1, the current "active" status of the triplet.
ranked_vouches as (
  select rank() over (
      partition by solver, pool, sender
      order by evt_block_number desc, evt_index desc
    ) as rk,
    *
  from (
      select * from vouches
      union
      select * from invalidations
    ) as _
),
-- This will contain all latest active vouches,
-- but could still contain solvers with multiplicity > 1 for different pools.
-- Rank here again by solver, and time.
current_active_vouches as (
  select rank() over (
      partition by solver
      order by evt_block_number, evt_index
    ) as time_rank,
    *
  from ranked_vouches
  where rk = 1
    and active = True
),
-- To filter for the case of "same solver, different pool",
-- rank the current_active vouches and choose the earliest
valid_vouches as (
  select
    solver,
    reward_target,
    pool
  from current_active_vouches
  where time_rank = 1
),
named_results as (
    select
        solver,
        concat(environment, '-', s.name) as solver_name,
        reward_target,
        vv.pool as bonding_pool,
        bp.name as pool_name
    from valid_vouches vv
    join cow_protocol_ethereum.solvers s
        on address = solver
    join bonding_pools bp
        on vv.pool = bp.pool
),
-- END VOUCH_REGISTRY

-- BEGIN SLIPPAGE: https://dune.com/queries/3427730
-- https://github.com/cowprotocol/solver-rewards/pull/342
block_range as (
    select
        min("number") as start_block,
        max("number") as end_block
    from ethereum.blocks
    where time >= cast('{{StartTime}}' as timestamp) and time < cast('{{EndTime}}' as timestamp)
)
,batch_meta as (
    select b.block_time,
           b.block_number,
           b.tx_hash,
           case
            when dex_swaps = 0
            -- Estimation made here: https://dune.com/queries/1646084
                then cast((gas_used - 73688 - (70528 * num_trades)) / 90000 as int)
                else dex_swaps
           end as dex_swaps,
           num_trades,
           b.solver_address
    from cow_protocol_ethereum.batches b
    where b.block_number >= (select start_block from block_range) and b.block_number <= (select end_block from block_range)
    and (b.solver_address = from_hex('{{SolverAddress}}') or '{{SolverAddress}}' = '0x')
    and (b.tx_hash = from_hex('{{TxHash}}') or '{{TxHash}}' = '0x')
)
,filtered_trades as (
    select t.tx_hash,
           b.block_number,
           case
                when trader = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                then 0x0000000000000000000000000000000000000001
                else trader
           end as trader_in,
           receiver                                     as trader_out,
           sell_token_address                           as sell_token,
           buy_token_address                            as buy_token,
           atoms_sold - coalesce(surplus_fee, cast(0 as uint256))        as atoms_sold,
           atoms_bought,
           0x9008d19f58aabd9ed0d60971565aa8510560ab41 as contract_address
    from cow_protocol_ethereum.trades t
         join cow_protocol_ethereum.batches b
            on t.tx_hash = b.tx_hash
    left outer join cow_protocol_ethereum.order_rewards f
        on f.tx_hash = t.tx_hash
        and f.order_uid = t.order_uid
    where b.block_number >= (select start_block from block_range) and b.block_number <= (select end_block from block_range)
    and t.block_number >= (select start_block from block_range) and t.block_number <= (select end_block from block_range)
    and (b.solver_address = from_hex('{{SolverAddress}}') or '{{SolverAddress}}' = '0x')
    and (t.tx_hash = from_hex('{{TxHash}}') or '{{TxHash}}' = '0x')
)
,batchwise_traders as (
    select
        tx_hash,
        block_number,
        array_agg(trader_in) as traders_in,
        array_agg(trader_out) as traders_out
    from filtered_trades
    group by tx_hash, block_number
)
,user_in as (
    select
        tx_hash,
        trader_in        as sender,
        contract_address as receiver,
        sell_token       as token,
        cast(atoms_sold as int256)       as amount_wei,
        'IN_USER'        as transfer_type
    from filtered_trades
)
,user_out as (
    select tx_hash,
          contract_address as sender,
          trader_out       as receiver,
          buy_token        as token,
          cast(atoms_bought as int256)            as amount_wei,
          'OUT_USER'       as transfer_type
    from filtered_trades
)
,other_transfers as (
    select b.tx_hash,
          "from"             as sender,
          to                 as receiver,
          t.contract_address as token,
          cast(value as int256) as amount_wei,
          case
              when to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                  then 'IN_AMM'
              when "from" = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                  then 'OUT_AMM'
              end            as transfer_type
    from erc20_ethereum.evt_Transfer t
             inner join cow_protocol_ethereum.batches b
                on evt_block_number = b.block_number
                and evt_tx_hash = b.tx_hash
             inner join batchwise_traders bt
                on evt_tx_hash = bt.tx_hash
    where b.block_number >= (select start_block from block_range) and b.block_number <= (select end_block from block_range)
      and 0x9008d19f58aabd9ed0d60971565aa8510560ab41 in (to, "from")
      and not contains(traders_in, "from")
      and not contains(traders_out, to)
      and to != "from"
      and "from" not in ( -- ETH FLOW ORDERS ARE NOT AMM TRANSFERS!
          select distinct contract_address
          from cow_protocol_ethereum.CoWSwapEthFlow_evt_OrderPlacement
      )
      and (t.evt_tx_hash = from_hex('{{TxHash}}') or '{{TxHash}}' = '0x')
      and (solver_address = from_hex('{{SolverAddress}}') or '{{SolverAddress}}' = '0x')
)
,eth_transfers as (
    select
        bt.tx_hash,
        "from" as sender,
        to     as receiver,
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token,
        cast(value as int256) as amount_wei,
        case
          when 0x9008d19f58aabd9ed0d60971565aa8510560ab41 = to
          then 'AMM_IN'
          else 'AMM_OUT'
        end as transfer_type
    from batchwise_traders bt
    inner join ethereum.traces et
        on bt.block_number = et.block_number
        and bt.tx_hash = et.tx_hash
        and value > cast(0 as uint256)
        and success = true
    and 0x9008d19f58aabd9ed0d60971565aa8510560ab41 in (to, "from")
    -- WETH unwraps don't have cancelling WETH transfer.
    and not 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 in (to, "from")
    -- ETH transfers to traders are already part of USER_OUT
    and not contains(traders_out, to)
)
-- sDAI emits only one transfer event for deposits and withdrawals.
-- This reconstructs the missing transfer from event logs.
,sdai_deposit_withdrawal_transfers as (
    -- withdraw events result in additional AMM_IN transfer
    select
        tx_hash,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        contract_address as token,
        cast(shares as int256) as amount_wei,
        'AMM_IN' as transfer_type
    from batch_meta bm
    join maker_ethereum.SavingsDai_evt_Withdraw w
    on w.evt_tx_hash= bm.tx_hash
    where owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
    union all
    -- deposit events result in additional AMM_OUT transfer
    select
        tx_hash,
        0x0000000000000000000000000000000000000000 as sender,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as receiver,
        contract_address as token,
        cast(shares as int256) as amount_wei,
        'AMM_OUT' as transfer_type
    from batch_meta bm
    join maker_ethereum.SavingsDai_evt_Deposit w
    on w.evt_tx_hash= bm.tx_hash
    where owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
)
,pre_batch_transfers as (
    select * from (
        select * from user_in
        union all
        select * from user_out
        union all
        select * from other_transfers
        union all
        select * from eth_transfers
        union all
        select * from sdai_deposit_withdrawal_transfers
        ) as _
    order by tx_hash
)
,batch_transfers as (
    select
        block_time,
        block_number,
        pbt.tx_hash,
        dex_swaps,
        num_trades,
        solver_address,
        sender,
        receiver,
        token,
        amount_wei,
        transfer_type
    from batch_meta bm
    join pre_batch_transfers pbt
        on bm.tx_hash = pbt.tx_hash
)
,incoming_and_outgoing as (
    SELECT
        block_time,
        tx_hash,
        dex_swaps,
        solver_address,
        case
            when t.symbol = 'ETH' then 'WETH'
            when t.symbol is not null then t.symbol
            else cast(i.token as varchar)
        end                                     as symbol,
          case
              when token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                  then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
              else token
              end                                     as token,
          case
              when receiver = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                  then amount_wei
              when sender = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                  then cast(-1 as int256) * amount_wei
              end                                     as amount,
          transfer_type
    from batch_transfers i
        left outer join tokens.erc20 t
            on i.token = t.contract_address
            and blockchain = 'ethereum'
)
-- -- V3 PoC Query For Token List: https://dune.com/queries/2259926
,token_list as (
    SELECT from_hex(address_str) as address
    FROM ( VALUES {{TokenList}} ) as _ (address_str)
)
,internalized_imbalances as (
  select  b.block_time,
          b.tx_hash,
          b.solver_address,
          t.symbol,
          from_hex(i.token) as token,
          cast(cast(i.amount as varchar) as int256) as amount,
          'PHANTOM_TRANSFER' as transfer_type
    from cowswap.raw_internal_imbalance i
    inner join cow_protocol_ethereum.batches b
        on i.block_number = b.block_number
        and from_hex(i.tx_hash) = b.tx_hash
    join tokens.erc20 t
        on contract_address = from_hex(token)
        and blockchain = 'ethereum'
    where i.block_number >= (select start_block from block_range) and i.block_number <= (select end_block from block_range)
    and ('{{SolverAddress}}' = '0x' or b.solver_address = from_hex('{{SolverAddress}}'))
    and ('{{TxHash}}' = '0x' or b.tx_hash = from_hex('{{TxHash}}'))
)
,incoming_and_outgoing_with_internalized_imbalances_temp as (
    select * from (
        select block_time,
              tx_hash,
              solver_address,
              symbol,
              token,
              amount,
              transfer_type
        from incoming_and_outgoing
        union all
        select * from internalized_imbalances
    ) as _
    order by block_time
)
-- add correction for protocol fees
,raw_protocol_fee_data as (
    select
        order_uid,
        tx_hash,
        cast(cast(data.protocol_fee as varchar) as int256) as protocol_fee,
        data.protocol_fee_token as protocol_fee_token,
        cast(cast(data.surplus_fee as varchar) as int256) as surplus_fee,
        solver,
        symbol
    from cowswap.raw_order_rewards ror
    join tokens.erc20 t
        on t.contract_address = from_hex(ror.data.protocol_fee_token)
        and blockchain = 'ethereum'
    where
        block_number >= (select start_block from block_range) and block_number <= (select end_block from block_range)
        and data.protocol_fee_native_price > 0
)
,buy_token_imbalance_due_to_protocol_fee as (
    select
        t.block_time as block_time,
        from_hex(r.tx_hash) as tx_hash,
        from_hex(r.solver) as solver_address,
        symbol,
        t.buy_token_address as token,
        (-1) * r.protocol_fee as amount,
        'protocol_fee_correction' as transfer_type
    from raw_protocol_fee_data r
    join cow_protocol_ethereum.trades t
        on from_hex(r.order_uid) = t.order_uid and from_hex(r.tx_hash) = t.tx_hash
    where t.order_type='SELL'
)
,sell_token_imbalance_due_to_protocol_fee as (
    select
        t.block_time as block_time,
        from_hex(r.tx_hash) as tx_hash,
        from_hex(r.solver) as solver_address,
        symbol,
        t.sell_token_address as token,
        r.protocol_fee * (t.atoms_sold - r.surplus_fee) / t.atoms_bought as amount,
        'protocol_fee_correction' as transfer_type
    from raw_protocol_fee_data r
    join cow_protocol_ethereum.trades t
        on from_hex(r.order_uid) = t.order_uid and from_hex(r.tx_hash) = t.tx_hash
    where t.order_type='SELL'
)
,incoming_and_outgoing_with_internalized_imbalances_unmerged as (
    select * from (
        select * from incoming_and_outgoing_with_internalized_imbalances_temp
        union all
        select * from buy_token_imbalance_due_to_protocol_fee
        union all
        select * from sell_token_imbalance_due_to_protocol_fee
    ) as _
    order by block_time
)
,incoming_and_outgoing_with_internalized_imbalances as (
    select
        block_time,
        tx_hash,
        solver_address,
        symbol,
        CASE
            WHEN token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            ELSE token
        END as token,
        amount,
        transfer_type
    from incoming_and_outgoing_with_internalized_imbalances_unmerged
)
-- These batches are excluded due to inaccurate prices.
,excluded_batches as (
    select tx_hash from query_3490353
)
,final_token_balance_sheet as (
    select
        solver_address,
        sum(amount) token_imbalance_wei,
        symbol,
        token,
        tx_hash,
        date_trunc('hour', block_time) as hour
    from
        incoming_and_outgoing_with_internalized_imbalances
    where tx_hash not in (select tx_hash from excluded_batches)
    group by
        symbol, token, solver_address, tx_hash, block_time
    having
        sum(amount) != cast(0 as int256)
)
,token_times as (
    select hour, token
    from final_token_balance_sheet
    group by hour, token
)
,precise_prices as (
    select
        contract_address,
        decimals,
        date_trunc('hour', minute) as hour,
        avg(
            CASE 
                WHEN (price > 10 and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab) then 0.26 -- dirty fix for some bogus COW prices Dune reports on July 29, 2024
                ELSE price
            END
        ) as price
    from
        prices.usd pusd
    inner join token_times tt
        on minute between date(hour) and date(hour) + interval '1' day -- query execution speed optimization since minute is indexed
        and date_trunc('hour', minute) = hour
        and contract_address = token
        and blockchain = 'ethereum'
    group by
        contract_address,
        decimals,
        date_trunc('hour', minute)
)
,intrinsic_prices as (
    select
        contract_address,
        decimals,
        hour,
        AVG(price) as price
    from (
        select
            buy_token_address as contract_address,
            ROUND(LOG(10, atoms_bought / units_bought)) as decimals,
            date_trunc('hour', block_time) as hour,
            usd_value / units_bought as price
        FROM cow_protocol_ethereum.trades
        WHERE block_number >= (select start_block from block_range) and block_number <= (select end_block from block_range)
        AND units_bought > 0
    UNION
        select
            sell_token_address as contract_address,
            ROUND(LOG(10, atoms_sold / units_sold)) as decimals,
            date_trunc('hour', block_time) as hour,
            usd_value / units_sold as price
        FROM cow_protocol_ethereum.trades
        WHERE block_number >= (select start_block from block_range) and block_number <= (select end_block from block_range)
        AND units_sold > 0
    ) as combined
    GROUP BY hour, contract_address, decimals
    order by hour
)
-- -- Price Construction: https://dune.com/queries/1579091?
,prices as (
    select
        tt.hour as hour,
        tt.token as contract_address,
        COALESCE(
            precise.decimals,
            intrinsic.decimals
        ) as decimals,
        COALESCE(
            precise.price,
            intrinsic.price
        ) as price
    from token_times tt
    LEFT JOIN precise_prices precise
        ON precise.hour = tt.hour
        AND precise.contract_address = token
    LEFT JOIN intrinsic_prices intrinsic
        ON intrinsic.hour = tt.hour
        and intrinsic.contract_address = token
)
-- -- ETH Prices: https://dune.com/queries/1578626?d=1
,eth_prices as (
    select
        date_trunc('hour', minute) as hour,
        avg(price) as eth_price
    from prices.usd
    where blockchain = 'ethereum'
    and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
    and minute between cast('{{StartTime}}' as timestamp) and cast('{{EndTime}}' as timestamp)
    group by date_trunc('hour', minute)
)
,results_per_tx as (
    select
        ftbs.hour,
        tx_hash,
        solver_address,
        sum(cast(token_imbalance_wei as double) * price / pow(10, p.decimals)) as usd_value,
        sum(cast(token_imbalance_wei as double) * price / pow(10, p.decimals) / eth_price) * pow(10, 18) as eth_slippage_wei,
        count(*) as num_entries
    from
        final_token_balance_sheet ftbs
    left join prices p
        on token = p.contract_address
        and p.hour = ftbs.hour
    left join eth_prices ep
        on ftbs.hour = ep.hour
    group by
        ftbs.hour,
        solver_address,
        tx_hash
    having
        bool_and(price is not null)
)
,solver_slippage as (
    select 
        solver_address as solver,
        sum(eth_slippage_wei) * 1.0 / pow(10, 18) as slippage
    from results_per_tx
    group by solver_address
)
-- END SLIPPAGE

-- BEGIN SOLVER REWARDS: https://dune.com/queries/2283297
,batch_rewards_temp as (
    select 
        block_deadline, 
        block_number, -- Null here means the settlement did not occur.
        from_hex(solver) as winning_solver,
        from_hex(tx_hash) as tx_hash,
        -- Unpacking the data
        cast(cast(data.winning_score as varchar) as int256) as winning_score,
        cast(cast(data.reference_score as varchar) as int256) as reference_score,
        cast(cast(data.surplus as varchar) as int256) as surplus,
        cast(cast(data.fee as varchar) as int256) as fee,
        cast(cast(data.execution_cost as varchar) as int256) as execution_cost,
        cast(cast(data.uncapped_payment_eth as varchar) as int256) as uncapped_payment_eth,
        CASE
            WHEN (block_deadline >= 20413283 and block_deadline <= 20413965 and cast(cast(data.capped_payment as varchar) as int256) < 0) then 0
            ELSE cast(cast(data.capped_payment as varchar) as int256)
        END as capped_payment,
        transform(data.participating_solvers, x -> from_hex(x)) as participating_solvers,
        cardinality(data.participating_solvers) as num_participants
    from cowswap.raw_batch_rewards
    WHERE block_deadline > (select start_block from block_range)
    AND block_deadline <= (select end_block from block_range)
)
,batch_rewards as (
    select
        block_deadline,
        block_number,
        winning_solver,
        tx_hash,
        winning_score,
        reference_score,
        surplus,
        --CASE
        --    WHEN block_deadline <= 19468663 AND block_number is NOT null AND block_number <= block_deadline THEN execution_cost
        --    ELSE fee
        --END as fee,
        CASE
            WHEN block_deadline <= 19468663 AND block_number is NOT null THEN execution_cost
            ELSE fee
        END as fee,
        execution_cost,
        --CASE
        --    WHEN block_deadline <= 19468663 AND block_number is NOT null AND block_number <= block_deadline THEN capped_payment - execution_cost
        --    ELSE capped_payment
        --END as capped_payment,
        CASE
            WHEN block_deadline <= 19468663 AND block_number is NOT null THEN capped_payment - execution_cost
            ELSE capped_payment
        END as capped_payment,
        participating_solvers,
        num_participants
    from batch_rewards_temp
)
,participation_data as (
    SELECT 
        tx_hash,
        participant,
        CASE
            WHEN block_deadline <= 20365510 THEN 1  -- final block deadline of accounting week of July 16 - July 23, 2024
            ELSE 0
        END as participation_count
    FROM batch_rewards br
    CROSS JOIN UNNEST(br.participating_solvers) AS t(participant)
),

participation_counts as (
    SELECT 
        participant as solver, 
        sum(participation_count) as num_participating_batches
    FROM participation_data
    group by participant
),

-- AKA Performance Rewards
primary_rewards as (
    SELECT
        winning_solver as solver,
        cast(SUM(capped_payment) as double) as reward_wei
        -- ,cast(SUM(execution_cost) as double) as exececution_cost_wei
    FROM batch_rewards
    GROUP BY winning_solver
),

fees_and_costs as (
    SELECT
        winning_solver as solver,
        cast(SUM(fee) as double) as network_fee_wei,
        cast(SUM(execution_cost) as double) as exececution_cost_wei
    FROM batch_rewards
    GROUP BY winning_solver
),
conversion_prices as (
    select
        (
            select avg(
                CASE WHEN price > 10 then 0.26 -- dirty fix for some bogus COW prices Dune reports on July 29, 2024
                ELSE price
                END
            ) from prices.usd 
            where blockchain = 'ethereum' 
            and contract_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
            and date(minute) = cast('{{EndTime}}' as timestamp) - interval '1' day
        ) as cow_price,
        (
            select avg(price) from prices.usd 
            where blockchain = 'ethereum' 
            and contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            and date(minute) = cast('{{EndTime}}' as timestamp) - interval '1' day
        ) as eth_price
)

-- BEGIN QUOTE REWARDS
,order_quotes as (SELECT order_uid,
                         quote_solver
                  FROM cow_protocol_ethereum.order_rewards
                  WHERE block_number > (select start_block from block_range)
                  AND block_number <= (select end_block from block_range)
)
,winning_quotes as (SELECT oq.order_uid,
                        quote_solver as solver
                    FROM order_quotes oq
                        INNER JOIN cow_protocol_ethereum.trades t ON oq.order_uid = t.order_uid
                    AND oq.quote_solver != 0x0000000000000000000000000000000000000000
)
,quote_rewards as (SELECT solver,
                           least({{QUOTE_REWARD}}, {{QUOTE_CAP_ETH}} * (select eth_price / cow_price from conversion_prices)) * count(*) as quote_reward
                    FROM winning_quotes
                    GROUP BY solver
),
                 
pre_results as (
    SELECT 
        pc.solver,
        coalesce(reward_wei, 0) / pow(10, 18) as reward_eth,
        coalesce(network_fee_wei, 0) / pow(10, 18) as network_fee_eth,
        coalesce(exececution_cost_wei, 0) / pow(10, 18) as execution_cost_eth,
        coalesce(reward_wei, 0) / pow(10, 18) * (select eth_price / cow_price from conversion_prices) as reward_cow,
        num_participating_batches
    FROM participation_counts pc
    LEFT OUTER JOIN primary_rewards pr
    ON pr.solver = pc.solver
    LEFT OUTER JOIN fees_and_costs fc
    ON fc.solver = pc.solver
),

participation_allocation_temp as (
    select
        greatest(
            least(
                {{PERIOD_BUDGET_COW}} - (select sum(reward_cow) from pre_results),
                {{CONSISTENCY_CAP_ETH}} * (select eth_price / cow_price from conversion_prices)),
            0) as total_allocation,
    (select sum(num_participating_batches) from participation_counts) as total_participation
),
participation_allocation as (
    select
        total_allocation,
        CASE
            WHEN total_participation > 0 THEN total_participation
            ELSE 1
        END as total_participation
    from participation_allocation_temp
),

aggregate_results as (
    select 
        solver,
        -- payment_eth,
        reward_eth as primary_reward_eth,
        reward_cow as primary_reward_cow,
        network_fee_eth,
        execution_cost_eth,
        num_participating_batches * (select total_allocation / total_participation from participation_allocation) as secondary_reward_cow,
        num_participating_batches * (select total_allocation / total_participation from participation_allocation) * (select cow_price / eth_price from conversion_prices)  as secondary_reward_eth,
        num_participating_batches
    from pre_results
),

service_fee_data as (
    select
        solver,
        service_fee
    from query_4038102
),

combined_data as (
    select 
        coalesce(ar.solver, ss.solver, qr.solver) as solver,
        -- coalesce(ar.solver, qr.solver) as solver,
        -- payment_eth,
        network_fee_eth,
        execution_cost_eth,
        primary_reward_eth,
        primary_reward_cow,
        secondary_reward_cow,
        secondary_reward_eth,
        num_participating_batches,
        coalesce(quote_reward, 0) as quote_reward,
        coalesce(slippage, 0) as slippage_eth,
        -- 0 as slippage_eth,
        concat(
            '<a href="https://dune.com/queries/2332678?SolverAddress=',
            cast(ar.solver as varchar),
            '&CTE_NAME=results_per_tx&StartTime={{StartTime}}&EndTime={{EndTime}}&MinAbsoluteSlippageTolerance=0&RelativeSlippageTolerance=0&SignificantSlippageValue=0" target="_blank">link</a>'
        ) as slippage_per_tx,
        concat(environment, '-', name) as name,
        CASE
            WHEN service_fee is true then 0.15
            else 0
        END as service_fee_factor
    from aggregate_results ar
    full outer join solver_slippage ss
        on ar.solver = ss.solver
    full outer join quote_rewards qr
        on ar.solver = qr.solver
    left join cow_protocol_ethereum.solvers s
        on coalesce(ar.solver, ss.solver, qr.solver) = address
        -- on coalesce(ar.solver, qr.solver) = address
    left join service_fee_data sfd on ar.solver = sfd.solver
)

,extended_payout_data as (
    select 
        solver,
        network_fee_eth,
        execution_cost_eth,
        primary_reward_eth,
        primary_reward_cow,
        secondary_reward_cow,
        secondary_reward_eth,
        num_participating_batches,
        (1 - service_fee_factor) * quote_reward as quote_reward,
        slippage_eth,
        slippage_per_tx,
        name,
        service_fee_factor,
        -- computed fields used to simplify case logic.
        (1 - service_fee_factor) *  (primary_reward_eth + secondary_reward_eth) + slippage_eth + network_fee_eth as total_outgoing_eth,
        case when (1 - service_fee_factor) *  (primary_reward_eth + secondary_reward_eth) + slippage_eth + network_fee_eth < 0 then true else false end as is_overdraft,
        slippage_eth + network_fee_eth as reimbursement_eth,
        (slippage_eth + network_fee_eth) * (select eth_price / cow_price from conversion_prices) as reimbursement_cow,
        (1 - service_fee_factor) * (primary_reward_cow + secondary_reward_cow) as total_cow_reward,
        (1 - service_fee_factor) * (primary_reward_eth + secondary_reward_eth) as total_eth_reward
    from combined_data cd
)

-- Implement the logic contained in 
-- https://github.com/cowprotocol/solver-rewards/blob/9838116e5253263e78e5b5777106458b541beb71/src/fetch/payouts.py#L136-L217
,final_results as (
    select 
        epd.*,
        case 
            when is_overdraft then null
            when reimbursement_eth > 0 and total_cow_reward < 0
                then reimbursement_eth + total_eth_reward
            when reimbursement_eth < 0 and total_cow_reward > 0
                then 0
            else reimbursement_eth
        end as eth_transfer,
        case 
            when is_overdraft then null
            when reimbursement_eth > 0 and total_cow_reward < 0
                then 0
            when reimbursement_eth < 0 and total_cow_reward > 0
                then reimbursement_cow + total_cow_reward
            else total_cow_reward
        end as cow_transfer,
        case when is_overdraft then total_outgoing_eth else null end as overdraft,
        reward_target
    from extended_payout_data epd
        left join named_results nr
        on epd.solver = nr.solver
)

select * from {{CTE_NAME}}
