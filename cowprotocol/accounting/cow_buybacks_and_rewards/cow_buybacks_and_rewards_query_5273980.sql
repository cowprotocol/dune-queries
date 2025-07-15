with
txs_to_exclude as (
    select 0x7684ba7c81b539f5a54d1e9a55dadd2fac1e355356b7b7fe99fc597345c59402 as tx_hash --mainnet
)
, cow_token_address as (
    select
        blockchain
        , address
    from query_5454278
)
, rewards_safe as (
    select
        blockchain
        , address
    from query_5454283
)
, solver_cow_rewards as (
    select
        time
        , sum(cow_rewarded) as cow_rewarded
    from (
        select
            date_trunc('{{time_frequency}}', t.evt_block_time) as time
            , sum(t.value / pow(10, 18)) as cow_rewarded
        from erc20_ethereum.evt_transfer as t
        join (select distinct cowrewardtarget from cow_protocol_ethereum.vouchregister_evt_vouch) as v
            on t."to" = v.cowrewardtarget
        join cow_token_address as cow
            on t.contract_address = cow.address
            and cow.blockchain = 'ethereum'
        join rewards_safe as r
            on t."from" = r.address
            and r.blockchain = 'ethereum'
        where
            t.evt_block_time between timestamp'{{starttime}}' and timestamp'{{endtime}}'
            and t.evt_tx_hash not in (select tx_hash from txs_to_exclude)
        group by 1

        union all
        select
            date_trunc('{{time_frequency}}', t.evt_block_time) as time
            , sum(value / pow(10, 18)) as cow_rewarded
        from erc20_gnosis.evt_transfer t
        join (select distinct cowrewardtarget from cow_protocol_gnosis.vouchregister_evt_vouch) v
            on t."to" = v.cowrewardtarget
        join cow_token_address as cow
            on t.contract_address = cow.address
            and cow.blockchain = 'gnosis'
        join rewards_safe as r
            on t."from" = r.address
            and r.blockchain = 'gnosis'
        where
            t.evt_block_time between timestamp'{{starttime}}' and timestamp'{{endtime}}'
        group by 1

        union all
        select
            date_trunc('{{time_frequency}}', t.evt_block_time) as time
            , sum(value / pow(10, 18)) as cow_rewarded
        from erc20_base.evt_transfer t
        join (select distinct cowrewardtarget from cow_protocol_base.vouchregister_evt_vouch) v
            on t."to" = v.cowrewardtarget
        join cow_token_address cow
            on t.contract_address = cow.address
            and cow.blockchain = 'base'
        join rewards_safe r
            on t."from" = r.address
            and r.blockchain = 'base'
        where
            t.evt_block_time between timestamp'{{starttime}}' and timestamp'{{endtime}}'
        group by 1

        union all
        select
            date_trunc('{{time_frequency}}', t.evt_block_time) as time
            , sum(value / pow(10, 18)) as cow_rewarded
        from erc20_arbitrum.evt_transfer t
        join (select distinct cowrewardtarget from cow_protocol_arbitrum.vouchregister_evt_vouch) v
            on t."to" = v.cowrewardtarget
        join cow_token_address cow
            on t.contract_address = cow.address
            and cow.blockchain = 'arbitrum'
        join rewards_safe r
            on t."from" = r.address
            and r.blockchain = 'arbitrum'
        where
            t.evt_block_time between timestamp'{{starttime}}' and timestamp'{{endtime}}'
        group by 1
    )
    group by 1
)
, cow_buyback as (
    select
        date_trunc('{{time_frequency}}', block_date) as time
        , sum(units_bought) as cow_bought_back
    from
        cow_protocol_ethereum.trades
    where
        trader in (0xb64963f95215fde6510657e719bd832bb8bb941b, 0x523732d31b4432bcdd4baad108f7ebe54ad478b0
            , 0x616de58c011f8736fa20c7ae5352f7f6fb9f0669, 0x22af3d38e50ddedeb7c47f36fab321ec3bb72a76)
        and buy_token_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
        and sell_token_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        and project_contract_address = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
        and block_time between timestamp '{{starttime}}' and timestamp '{{endtime}}'
    group by 1
)
select
    coalesce(r.time, b.time) as time,
    r.cow_rewarded,
    sum(r.cow_rewarded) over (order by coalesce(r.time, b.time) nulls first) as cumulative_rewards,
    b.cow_bought_back,
    sum(b.cow_bought_back) over (order by coalesce(r.time, b.time) nulls first) as cumulative_buybacks,
    sum(r.cow_rewarded) over (order by coalesce(r.time, b.time) nulls first)
    - sum(b.cow_bought_back) over (order by coalesce(r.time, b.time) nulls first) as net_emissions
from solver_cow_rewards as r
full outer join cow_buyback as b
    on r.time = b.time
where
    coalesce(r.time, b.time) between timestamp '{{starttime}}' and timestamp '{{endtime}}'
order by 1 desc
