-- In the event that there are any results for this query,
-- "oncall" can manually perform refunds via:
-- https://github.com/cowprotocol/manual-ethflow-refunder
with
join_with_trade_events as (
    select
        sender,
        tx_hash as placement_tx,
        evt_tx_hash as fill_tx,
        block_time as placement_time,
        evt_block_time as fill_time,
        -- TODO fix this in the view!
        cast(valid_to as timestamp) as valid_to,
        block_number as placement_block,
        evt_block_number as fill_block,
        order_uid
    from cow_protocol_ethereum.eth_flow_orders
    left outer join gnosis_protocol_v2_ethereum.GPv2Settlement_evt_Trade
        on
            order_uid = orderUid
            and evt_block_time > block_time
    where block_time > now() - interval '1' day
),

cancellations as (
    select
        orderUid,
        evt_tx_hash as cancellation_tx,
        evt_block_time as cancellation_time,
        evt_block_number as cancellation_block,
        sum(cast(value as double) / pow(10, 18)) as refund_amount_eth
    from cow_protocol_ethereum.CoWSwapEthFlow_evt_OrderInvalidation
    inner join ethereum.tracess
        on
            evt_block_number = block_number
            and evt_tx_hash = tx_hash
    where evt_block_time > now() - interval '1' day
    group by orderUid, evt_tx_hash, evt_block_time, evt_block_number
),

-- select * from cancellations

refunds as (
    select
        evt_tx_hash as refund_tx,
        evt_block_time as refund_time,
        evt_block_number as refund_block,
        refunder,
        orderUid,
        sum(cast(value as double) / pow(10, 18)) as refund_amount_eth
    from cow_protocol_ethereum.CoWSwapEthFlow_evt_OrderRefund
    inner join ethereum.traces
        on
            evt_block_number = block_number
            and evt_tx_hash = tx_hash
    where evt_block_time > now() - interval '1' day
    group by orderUid, evt_tx_hash, evt_block_time, refunder, evt_block_number
),

-- select * from refunds

unfilled_orders as (
    select
        placement_tx,
        placement_time,
        valid_to,
        cancellation_time,
        cancellation_tx,
        refund_time,
        refund_tx,
        refunder,
        order_uid,
        coalesce(c.refund_amount_eth, r.refund_amount_eth) as refunded_amount,
        (
            case
                when now() > valid_to
                    then (to_unixtime(valid_to) - to_unixtime(placement_time))
                else (to_unixtime(now()) - to_unixtime(placement_time))
            end
        ) / 60 as time_open_minutes,
        cancellation_time is null and now() > valid_to as expired,
        cancellation_time is not null as canceled
    from join_with_trade_events
    left outer join cancellations as c
        on
            cancellation_block >= placement_block
            and order_uid = c.orderUid
    left outer join refunds as r
        on
            refund_block >= placement_block
            and order_uid = r.orderUid
    where fill_block is null
)

select * from unfilled_orders
where
    now() > valid_to + interval '{{grace_period}}' minute
    and expired = true
    and refund_tx is null
    and valid_to
    between
    cast('{{start_time}}' as timestamp) - interval '{{grace_period}}' minute
    and
    cast('{{end_time}}' as timestamp) - interval '{{grace_period}}' minute
