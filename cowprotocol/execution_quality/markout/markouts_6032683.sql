-- Markout comparison across multiple DEX aggregators and CoW Protocol
-- Returns average markout in basis points (bps) for each project over the specified time period

-- Parameters:
--  {{blockchain}} - The chain on which trades should be counted
--  {{start_date}} - Start date for when trades should be counted
--  {{end_date}} - End date for when trades should be counted
--  {{min_usd_amount}} - Minimum USD amount of the trade to be considered
--  {{max_usd_amount}} - Maximum USD amount of the trade to be considered

select
    '1Inch Fusion' as project
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031733(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}')"
union distinct
select
    'CoW' as project
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031665(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}')"
union distinct
select
    'Kyberswap' as project
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='kyberswap')"
union distinct
select
    'Paraswap' as project
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='paraswap')"
union distinct
select
    'Odos' as project
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='odos')"
union all
select 
    'Uniswap X' as project
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6033942(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}')"
union distinct
select 
    '1inch' as project -- non intent based
    , avg(oneinch.markout) * 10000 as avg_markout_bps
    , approx_percentile(oneinch.markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(oneinch.markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(oneinch.markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='1inch')" as oneinch
left join "query_6031733(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}')" as fusion
    on oneinch.tx_hash=fusion.tx_hash
where fusion.tx_hash is null
