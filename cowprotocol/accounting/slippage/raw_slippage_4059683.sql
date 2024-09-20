-- This query gives a breakdown of slippage on CoW Protocol
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--  {{raw_slippage_table_name}} - raw_slippage_breakdown for a detailed per token breakdown of
--    slippage; raw_slippage_per_transaction for aggregated values per transaction
--
-- The columns of raw_slippage_breakdown are
-- - block_time: time of settlement transaction
-- - tx_hash: settlement transaction hash
-- - token_address: address of token with slippage. contract address for erc20 tokens,
--   0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee for native token
-- - amount: signed value of slippage in atoms of the token; fees are represented as negative
--   integers since they will be removed from imbalances
-- - slippage_type: 'raw_imbalance' for imbalance observable on chain, 'protocol_fee' for the total
--   protocol fee (including partner fee), 'network_fee' for network fees
-- - price_unit: USD price of one unit (i.e. pow(10, decimals) atoms) of a token
-- - price_atom: USD price of one atom (i.e. 1. / pow(10, decimals) units) of a token
-- - slippage_usd: USD value of slippage
-- - slippage_wei: value of slippage in atoms of native token
--
-- The columns of raw_slippage_per_solver are
-- - block_time: time of settlement transaction
-- - tx_hash: settlement transaction hash
-- - slippage_usd: USD value of slippage
-- - slippage_wei: value of slippage in atoms of native token

with raw_token_imbalances as (
    select
        block_time,
        tx_hash,
        token_address,
        amount,
        'raw_imbalance' as slippage_type,
        date_trunc('hour', block_time) as hour --noqa: RF04
    from "query_4021644(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

fees as (
    select -- noqa: ST06
        block_time,
        tx_hash,
        token_address,
        -amount as amount,
        fee_type as slippage_type,
        date_trunc('hour', block_time) as hour --noqa: RF04
    from "query_4058574(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

imbalances as (
    select * from raw_token_imbalances
    union all
    select * from fees
),

prices as (
    select * from "query_4064601(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

raw_slippage_breakdown as (
    select -- noqa: ST06
        block_time,
        tx_hash,
        i.token_address,
        amount as slippage_atoms,
        slippage_type,
        p.price_unit,
        p.price_atom,
        amount * p.price_atom as slippage_usd,
        cast(amount * p.price_atom / np.price_atom as int256) as slippage_wei
    from
        imbalances as i
    left join prices as p
        on
            i.token_address = p.token_address
            and i.hour = p.hour
    left join prices as np
        on
            i.hour = np.hour
            and np.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
),

raw_slippage_per_transaction as (
    select
        block_time,
        tx_hash,
        sum(slippage_usd) as slippage_usd,
        sum(slippage_wei) as slippage_wei
    from raw_slippage_breakdown
    group by 1, 2
)

select * from {{raw_slippage_table_name}}
