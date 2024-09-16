-- This query a breakdown of slippage on CoW Protocol
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on
--
-- The columns of the result are
-- - block_time: time of settlement transaction
-- - tx_hash: settlement transaction hash
-- - token_address: address of token with slippage. contract address for erc20 tokens,
--   0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee for native token
-- - amount: signed value of slippage in atoms of the token; fees are represented as negative
--   integers since they will be removed from imbalances
-- - price: USD price of one unit (i.e. pow(10, decimals) atoms) of a token
-- - price_atom: USD price of one atom (i.e. 1. / pow(10, decimals) units) of a token
-- - slippage_usd: USD value of slippage
-- - slippage_native_atom: value of slippage in atoms of native token
-- - transfer_type: 'raw_imbalance' for imbalance observable on chain, 'protocol_fee' for the total
--   protocol fee (including partner fee), 'network_fee' for network fees

with raw_token_imbalances as (
    select
        block_time,
        tx_hash,
        token_address,
        amount,
        'raw_imbalance' as transfer_type,
        date_trunc('hour', block_time) as hour --noqa: RF04
    from "query_4021644(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

fee_balance_changes as (
    select
        block_time,
        tx_hash,
        token_address,
        transfer_type,
        -amount as amount,
        date_trunc('hour', block_time) as hour --noqa: RF04
    from "query_4058574(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
),

raw_slippage as (
    select * from raw_token_imbalances
    union all
    select * from fee_balance_changes
),

prices as (
    select * from "query_4064601(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
)

select
    block_time,
    tx_hash,
    rs.token_address,
    amount as slippage_atoms,
    p.price,
    p.price_atom,
    amount * p.price_atom as slippage_usd,
    cast(amount * p.price_atom / np.price_atom as int256) as slippage_native_atom,
    transfer_type
from
    raw_slippage as rs
left join prices as p
    on rs.token_address = p.token_address
    and rs.hour = p.hour
left join prices as np
    on rs.hour = np.hour
    and np.token_address = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee