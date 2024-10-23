-- Computes the value MEV Blocker transactions contribute for each block
-- Parameters:
--  {{start}} - the timestamp for which the analysis should start (inclusively)
--  {{end}} - the timestamp for which the analysis should end (exclusively)

select
    block_time,
    block_number,
    sum(user_tip_wei) AS user_tip_wei,
    sum(backrun_value_wei) AS backrun_value_wei,
    sum(backrun_tip_wei) AS backrun_tip_wei,
    array_agg(hash) as user_txs,
    array_agg(searcher_txs) as searcher_txs,
    array_agg(kickback_txs) as kickback_txs,
    sum(tx_mevblocker_fee_wei) AS block_fee_wei
from "query_4188777(start='{{start}}', end='{{end}}', referrer='%')"
group by 1,2
