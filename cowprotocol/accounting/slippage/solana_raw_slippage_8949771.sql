-- Raw slippage on CoW Protocol for Solana
--
-- Parameters:
--  {{start_time}} - start of the analysis window (inclusive)
--  {{end_time}}   - end of the analysis window (exclusive)
--  {{raw_slippage_table_name}} - raw_slippage_breakdown for a per-token breakdown;
--    raw_slippage_per_transaction for per-transaction totals
--
-- raw_slippage_breakdown columns:
-- - block_time, tx_id:  settlement transaction
-- - token_address:      SPL mint (base58); 11111111111111111111111111111111 for native SOL
-- - slippage_type:      'raw_imbalance' (buffer balance change observable on chain)
-- - slippage_units:     signed imbalance in token units (decimal-adjusted)
-- - price_unit:         USD price of one unit
-- - slippage_usd:       USD value of the imbalance (null if no price)
--
-- raw_slippage_per_transaction columns:
-- - block_time, tx_id
-- - slippage_usd:       USD value of priced imbalances
-- - unpriced_tokens:    number of tokens in the tx with no price (slippage_usd is then incomplete)

with raw_token_imbalances as (
    select
        block_time,
        tx_id,
        token_address,
        amount,
        'raw_imbalance' as slippage_type,
        date_trunc('hour', block_time) as hour --noqa: RF04
    from "query_8863696(start_time='{{start_time}}',end_time='{{end_time}}')"
),

prices as (
    select * from "query_8949765(start_time='{{start_time}}',end_time='{{end_time}}')"
),

raw_slippage_breakdown as (
    select
        i.block_time,
        i.tx_id,
        i.token_address,
        i.slippage_type,
        i.amount               as slippage_atoms,
        p.price_unit,
        i.amount * p.price_atom as slippage_usd
    from raw_token_imbalances as i
    left join prices as p
        on i.token_address = p.token_address
        and i.hour = p.hour
),

raw_slippage_per_transaction as (
    select
        block_time,
        tx_id,
        sum(slippage_usd)                as slippage_usd,
        count_if(slippage_usd is null)   as unpriced_tokens
    from raw_slippage_breakdown
    group by 1, 2
)

select * from {{raw_slippage_table_name}}
order by block_time desc
