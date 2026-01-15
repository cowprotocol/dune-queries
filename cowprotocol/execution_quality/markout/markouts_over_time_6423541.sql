-- Markout comparison across multiple DEX aggregators and CoW Protocol over time
-- Returns markout avg and percentiles in basis points (bps) for each project over the specified time period

-- Parameters:
--  {{blockchain}} - The chain on which trades should be counted
--  {{start_date}} - Start date for when trades should be counted
--  {{end_date}} - End date for when trades should be counted
--  {{min_usd_amount}} - Minimum USD amount of the trade to be considered
--  {{max_usd_amount}} - Maximum USD amount of the trade to be considered

select
    'CoW' as project
    , date_trunc('{{date_granularity}}', block_time) as date
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031665(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}')"
group by 1 , 2
union all 
select
    '1inch (intent based)' as project
    , date_trunc('{{date_granularity}}', block_time) as date
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031733(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}')"
group by 1 , 2
union all 
select 
    'Uniswap X' as project
    , date_trunc('{{date_granularity}}', block_time) as date
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6033942(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}')"
group by 1 , 2
union all 
select 
    '1inch (non intent based)' as project
    , date_trunc('{{date_granularity}}', block_time) as date
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='1inch')"
group by 1 , 2
union all 
select
    'Kyberswap' as project
    , date_trunc('{{date_granularity}}', block_time) as date
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='kyberswap')"
group by 1 , 2
union all 
select
    'Paraswap' as project
    , date_trunc('{{date_granularity}}', block_time) as date
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='paraswap')"
group by 1 , 2
union all 
select
    'Odos' as project
    , date_trunc('{{date_granularity}}', block_time) as date
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='odos')"
group by 1 , 2
union all 
select
    '0x' as project
    , date_trunc('{{date_granularity}}', block_time) as date
    , avg(markout) * 10000 as avg_markout_bps
    , approx_percentile(markout, 0.25) * 10000 as p25_markout_bps
    , approx_percentile(markout, 0.50) * 10000 as p50_markout_bps
    , approx_percentile(markout, 0.75) * 10000 as p75_markout_bps
from "query_6031688(blockchain='{{blockchain}}',start_date='{{start_date}}',end_date='{{end_date}}',min_usd_amount='{{min_usd_amount}}',max_usd_amount='{{max_usd_amount}}',project='0x')"
group by 1 , 2
