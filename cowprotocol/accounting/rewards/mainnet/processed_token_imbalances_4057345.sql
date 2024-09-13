with
block_range as (
    select * from "query_3333356(start_time='{{start_time}}',end_time='{{end_time}}')"
),

batch_meta as (
    select
        b.block_time,
        b.block_number,
        b.tx_hash,
        b.solver_address
    from cow_protocol_ethereum.batches as b
    where
        b.block_number >= (select start_block from block_range) and b.block_number <= (select end_block from block_range)
),

filtered_trades as (
    select
        t.tx_hash,
        b.block_number,
        receiver as trader_out,
        sell_token_address as sell_token,
        buy_token_address as buy_token,
        atoms_bought,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as contract_address,
        case
            when trader = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                then 0x0000000000000000000000000000000000000001
            else trader
        end as trader_in,
        -- here we aim to account for network fee. However, in case there is also a protocol fee
        -- the surplus_fee accounts for that so there needs to be a correction here, that happens
        -- a bit later on in this long query.
        atoms_sold - coalesce(surplus_fee, cast(0 as uint256)) as atoms_sold
    from cow_protocol_ethereum.trades as t
    inner join cow_protocol_ethereum.batches as b
        on t.tx_hash = b.tx_hash
    left outer join cow_protocol_ethereum.order_rewards as f
        on
            t.tx_hash = f.tx_hash
            and t.order_uid = f.order_uid
    where
        t.block_number >= (select start_block from block_range) and t.block_number <= (select end_block from block_range)
),

batchwise_traders as (
    select
        tx_hash,
        block_number,
        array_agg(trader_in) as traders_in,
        array_agg(trader_out) as traders_out
    from filtered_trades
    group by tx_hash, block_number
),

user_in as (
    select
        tx_hash,
        trader_in as sender,
        contract_address as receiver,
        sell_token as token,
        cast(atoms_sold as int256) as amount_wei,
        'IN_USER' as transfer_type
    from filtered_trades
),

user_out as (
    select
        tx_hash,
        contract_address as sender,
        trader_out as receiver,
        buy_token as token,
        cast(atoms_bought as int256) as amount_wei,
        'OUT_USER' as transfer_type
    from filtered_trades
),

other_transfers as (
    select
        b.tx_hash,
        "from" as sender,
        to as receiver,
        t.contract_address as token,
        cast(value as int256) as amount_wei,
        case
            when to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                then 'IN_AMM'
            when "from" = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                then 'OUT_AMM'
        end as transfer_type
    from erc20_ethereum.evt_Transfer as t
    inner join cow_protocol_ethereum.batches as b
        on
            evt_block_number = b.block_number
            and evt_tx_hash = b.tx_hash
    inner join batchwise_traders as bt
        on evt_tx_hash = bt.tx_hash
    where
        b.block_number >= (select start_block from block_range) and b.block_number <= (select end_block from block_range)
        and 0x9008d19f58aabd9ed0d60971565aa8510560ab41 in (to, "from")
        and not contains(traders_in, "from")
        and not contains(traders_out, to)
        and to != "from"
        and "from" not in ( -- ETH FLOW ORDERS ARE NOT AMM TRANSFERS!
            select distinct contract_address
            from cow_protocol_ethereum.CoWSwapEthFlow_evt_OrderPlacement
        )
),

eth_transfers as (
    select
        bt.tx_hash,
        "from" as sender,
        to as receiver,
        0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token,
        cast(value as int256) as amount_wei,
        case
            when 0x9008d19f58aabd9ed0d60971565aa8510560ab41 = to
                then 'AMM_IN'
            else 'AMM_OUT'
        end as transfer_type
    from batchwise_traders as bt
    inner join ethereum.traces as et
        on
            bt.block_number = et.block_number
            and bt.tx_hash = et.tx_hash
            and value > cast(0 as uint256)
            and success = true
            and 0x9008d19f58aabd9ed0d60971565aa8510560ab41 in (to, "from")
            -- WETH unwraps don't have cancelling WETH transfer.
            and not 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 in (to, "from")
            -- ETH transfers to traders are already part of USER_OUT
            and not contains(traders_out, to)
),

-- sDAI emits only one transfer event for deposits and withdrawals.
-- This reconstructs the missing transfer from event logs.
sdai_deposit_withdrawal_transfers as (
    -- withdraw events result in additional AMM_IN transfer
    select
        tx_hash,
        0x9008d19f58aabd9ed0d60971565aa8510560ab41 as sender,
        0x0000000000000000000000000000000000000000 as receiver,
        contract_address as token,
        cast(shares as int256) as amount_wei,
        'AMM_IN' as transfer_type
    from batch_meta as bm
    inner join maker_ethereum.SavingsDai_evt_Withdraw as w
        on bm.tx_hash = w.evt_tx_hash
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
    from batch_meta as bm
    inner join maker_ethereum.SavingsDai_evt_Deposit as w
        on bm.tx_hash = w.evt_tx_hash
    where owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),

all_transfers_temp as (
    select * from user_in
    union all
    select * from user_out
    union all
    select * from other_transfers
    union all
    select * from eth_transfers
    union all
    select * from sdai_deposit_withdrawal_transfers
),

batch_transfers as (
    select
        block_time,
        block_number,
        att.tx_hash,
        solver_address,
        sender,
        receiver,
        token,
        amount_wei,
        transfer_type
    from batch_meta as bm
    inner join all_transfers_temp as att
        on bm.tx_hash = att.tx_hash
),

incoming_and_outgoing_temp as (
    select
        block_time,
        tx_hash,
        solver_address,
        transfer_type,
        case
            when token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            else token
        end as token,
        case
            when receiver = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                then amount_wei
            when sender = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
                then cast(-1 as int256) * amount_wei
        end as amount
    from batch_transfers as i
    left outer join tokens.erc20 as t
        on
            i.token = t.contract_address
            and blockchain = 'ethereum'
),

incoming_and_outgoing as (
    select
        block_time,
        tx_hash,
        solver_address,
        token,
        amount,
        transfer_type
    from incoming_and_outgoing_temp
    order by block_time
),

-- add correction for protocol fees
raw_protocol_fee_data as (
    select
        order_uid,
        tx_hash,
        cast(cast(data.protocol_fee as varchar) as int256) as protocol_fee,
        data.protocol_fee_token,
        cast(cast(data.surplus_fee as varchar) as int256) as surplus_fee,
        solver,
    from cowswap.raw_order_rewards
    inner join tokens.erc20 as t
        on
            t.contract_address = from_hex(data.protocol_fee_token)
            and blockchain = 'ethereum'
    where
        block_number >= (select start_block from block_range) and block_number <= (select end_block from block_range)
        and data.protocol_fee_native_price > 0
),

buy_token_imbalance_due_to_protocol_fee as (
    select
        t.block_time,
        t.buy_token_address as token,
        'protocol_fee_correction' as transfer_type,
        from_hex(r.tx_hash) as tx_hash,
        from_hex(r.solver) as solver_address,
        (-1) * r.protocol_fee as amount
    from raw_protocol_fee_data as r
    inner join cow_protocol_ethereum.trades as t
        on from_hex(r.order_uid) = t.order_uid and from_hex(r.tx_hash) = t.tx_hash
    where t.order_type = 'SELL'
),

sell_token_imbalance_due_to_protocol_fee as (
    select
        t.block_time,
        t.sell_token_address as token,
        'protocol_fee_correction' as transfer_type,
        from_hex(r.tx_hash) as tx_hash,
        from_hex(r.solver) as solver_address,
        r.protocol_fee * (t.atoms_sold - r.surplus_fee) / t.atoms_bought as amount
    from raw_protocol_fee_data as r
    inner join cow_protocol_ethereum.trades as t
        on from_hex(r.order_uid) = t.order_uid and from_hex(r.tx_hash) = t.tx_hash
    where t.order_type = 'SELL'
),

incoming_and_outgoing_premerge as (
    select * from incoming_and_outgoing
    union all
    select * from buy_token_imbalance_due_to_protocol_fee
    union all
    select * from sell_token_imbalance_due_to_protocol_fee
),

incoming_and_outgoing_final as (
    select
        block_time,
        tx_hash,
        solver_address,
        amount,
        transfer_type,
        case
            when token = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee then 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
            else token
        end as token
    from incoming_and_outgoing_premerge
    order by block_time
),

-- These batches involve a token that either does not emit standard transfer events,
-- or has some inaccurate price in Dune.
excluded_batches as (
    select tx_hash from query_3490353
),

final_token_balance_sheet as (
    select
        solver_address,
        token,
        tx_hash,
        sum(amount) as token_imbalance_wei,
        date_trunc('hour', block_time) as hour
    from
        incoming_and_outgoing_final
    where tx_hash not in (select tx_hash from excluded_batches)
    group by
        token, solver_address, tx_hash, block_time
    having
        sum(amount) != cast(0 as int256)
)

select * from final_token_balance_sheet
