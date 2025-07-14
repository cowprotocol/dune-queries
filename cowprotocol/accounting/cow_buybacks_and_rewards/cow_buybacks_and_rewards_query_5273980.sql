WITH 
txs_to_exclude as (
    select 0x7684ba7c81b539f5a54d1e9a55dadd2fac1e355356b7b7fe99fc597345c59402 as tx_hash --mainnet
)
,cow_token_address as (        
    select * from query_5454278
)
,rewards_safe as (
    select * from query_5454283
)
,solver_cow_rewards AS (
    SELECT time, sum(cow_rewarded) as cow_rewarded
    FROM (
        SELECT 
            date_trunc('{{time_frequency}}', t.evt_block_time) AS time
            ,sum(value / pow(10, 18)) AS cow_rewarded
        FROM erc20_ethereum.evt_transfer t
        JOIN (select distinct cowRewardTarget from cow_protocol_ethereum.VouchRegister_evt_Vouch) v 
            ON t."to" = v.cowRewardTarget
        JOIN cow_token_address cow 
            ON t.contract_address = cow.address 
            AND cow.blockchain = 'ethereum'
        JOIN rewards_safe r 
            ON t."from" = r.address 
            AND r.blockchain = 'ethereum'
        WHERE 
            t.evt_block_time between timestamp'{{StartTime}}' AND timestamp'{{EndTime}}'
            and t.evt_tx_hash NOT IN (select tx_hash from txs_to_exclude) 
        GROUP BY 1 
    
        UNION ALL 
        SELECT 
            date_trunc('{{time_frequency}}', t.evt_block_time) AS time
            ,sum(value / pow(10, 18)) AS cow_rewarded
        FROM erc20_gnosis.evt_transfer t 
        JOIN (select distinct cowRewardTarget from cow_protocol_gnosis.VouchRegister_evt_Vouch) v 
            ON t."to" = v.cowRewardTarget
        JOIN cow_token_address cow 
            ON t.contract_address = cow.address 
            AND cow.blockchain = 'gnosis'
        JOIN rewards_safe r 
            ON t."from" = r.address 
            AND r.blockchain = 'gnosis'
        WHERE 
            t.evt_block_time between timestamp'{{StartTime}}' AND timestamp'{{EndTime}}'
        GROUP BY 1 
    
        UNION ALL 
        SELECT 
            date_trunc('{{time_frequency}}', t.evt_block_time) AS time
            ,sum(value / pow(10, 18)) AS cow_rewarded
        FROM erc20_base.evt_transfer t 
        JOIN (select distinct cowRewardTarget from cow_protocol_base.VouchRegister_evt_Vouch) v 
            ON t."to" = v.cowRewardTarget
        JOIN cow_token_address cow 
            ON t.contract_address = cow.address 
            AND cow.blockchain = 'base'
        JOIN rewards_safe r 
            ON t."from" = r.address 
            AND r.blockchain = 'base'
        WHERE 
            t.evt_block_time between timestamp'{{StartTime}}' AND timestamp'{{EndTime}}'
        GROUP BY 1 
    
        UNION ALL 
        SELECT 
            date_trunc('{{time_frequency}}', t.evt_block_time) AS time
            ,sum(value / pow(10, 18)) AS cow_rewarded
        FROM erc20_arbitrum.evt_transfer t 
        JOIN (select distinct cowRewardTarget from cow_protocol_arbitrum.VouchRegister_evt_Vouch) v 
            ON t."to" = v.cowRewardTarget
        JOIN cow_token_address cow 
            ON t.contract_address = cow.address 
            AND cow.blockchain = 'arbitrum'
        JOIN rewards_safe r 
            ON t."from" = r.address 
            AND r.blockchain = 'arbitrum'
        WHERE 
            t.evt_block_time between timestamp'{{StartTime}}' AND timestamp'{{EndTime}}'
        GROUP BY 1 
        )
    GROUP BY 1
)
, cow_buyback AS (
    select 
        date_trunc('{{time_frequency}}', block_date) AS time
        , sum(units_bought) as cow_bought_back            
    from 
        cow_protocol_ethereum.trades
    where
        trader IN (0xb64963f95215fde6510657e719bd832bb8bb941b, 0x523732d31B4432BcDD4BaaD108f7EBE54AD478b0, 0x616dE58c011F8736fa20c7Ae5352F7f6FB9F0669, 0x22af3D38E50ddedeb7C47f36faB321eC3Bb72A76)
        and buy_token_address = 0xdef1ca1fb7fbcdc777520aa7f396b4e015f497ab
        and sell_token_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        and project_contract_address = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
        and block_time between timestamp '{{StartTime}}' and timestamp '{{EndTime}}'
    group by 1
) 
SELECT 
    COALESCE(r.time, b.time) AS time,
    cow_rewarded, 
    SUM(cow_rewarded) OVER (ORDER BY COALESCE(r.time, b.time) NULLS FIRST) AS cumulative_rewards, 
    cow_bought_back,
    sum(cow_bought_back) OVER (ORDER BY COALESCE(r.time, b.time) NULLS FIRST) AS cumulative_buybacks, 
    SUM(cow_rewarded) OVER (ORDER BY COALESCE(r.time, b.time) NULLS FIRST) 
        - sum(cow_bought_back) OVER (ORDER BY COALESCE(r.time, b.time) NULLS FIRST) AS net_emissions
FROM solver_cow_rewards r
FULL OUTER JOIN cow_buyback b 
    ON b.time = r.time
WHERE
    COALESCE(r.time, b.time) between timestamp '{{StartTime}}' and timestamp '{{EndTime}}'
ORDER BY 1 DESC
