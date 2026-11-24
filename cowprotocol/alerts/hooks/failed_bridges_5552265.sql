-- Failed bridge alert: bridging orders whose own bridging post-hook reverted.
-- Window: the last {{days}} full UTC days, ending yesterday (today is excluded).
-- dune.cowprotocol.fct_trades is rebuilt daily at 06:00 UTC with trades up to the previous midnight,
-- so run the alert after that (08:00 UTC is fine) with days = 1.
-- bridging_provider comes from dim_app_data (full app data history), so any lookback works.
-- A reverted hook is tied to the order by tx_hash + the trader address in the hook calldata,
-- restricted to CoW Shed executeHooks (0xa8481abe).
-- Hook-based bridges only (Bungee, Across, Relay); Near Intents orders have no bridging hook.
with failed as (
    select
        blockchain
        ,tx_hash
        ,input
    from evms.traces
    where
        blockchain in ('ethereum', 'gnosis', 'base', 'arbitrum', 'avalanche_c', 'polygon', 'bnb', 'linea', 'plasma', 'ink') -- chains in fct_trades; prunes partitions
        and block_date >= current_date - interval '{{days}}' day
        and block_date < current_date
        and "from" in (0x01dcb88678aedd0c4cc9552b20f4718550250574, 0x60bf78233f48ec42ee3f101b9a05ec7878728006, 0xd496f9fcfba14d7bd1e45e4840d38ad85ded14dd) --hooks trampoline
        and not success
        and varbinary_substring(input, 1, 4) = 0xa8481abe -- CoW Shed executeHooks (all bridging hooks)
)
select distinct
    t.blockchain
    ,t.block_time
    ,t.tx_hash
    ,t.order_uid
    ,t.usd_value
    ,t.bridging_provider
    ,t.dst_chain
from dune.cowprotocol.fct_trades as t
inner join failed as f
    on f.blockchain = t.blockchain
    and f.tx_hash = t.tx_hash
    and varbinary_position(f.input, t.trader) > 0 -- the reverted hook belongs to this order's trader
where
    t.block_date >= current_date - interval '{{days}}' day
    and t.block_date < current_date
    and t.bridging_provider is not null
order by t.block_time
