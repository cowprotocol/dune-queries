-- Computes the volume weighted ratio between the markouts of two dex aggregator projects (compared to Dune prices) per token pair.
-- A value >1 means that project_a is providing better execution than project_b
--
-- Parameters:
--  {{project_a}} - The first aggregator to look at
--  {{project_b}} - The aggregator to compare with
--  {{start}} - Start date for when trades should be counted
--  {{blockchain}} - The chain on which trades should be counted
--  {{top_n_tokens}} - Based on trading volume (of project_a) how many of the top token pairs to consider

with comparison as(
    select 
        p1.buy_token, 
        p1.sell_token, 
        p1.dune_price_ratio as project_a_price_ratio, 
        p1.volume as project_a_volume, 
        p2.dune_price_ratio as project_b_price_ratio, 
        p2.volume as project_b_volume, 
        p1.dune_price_ratio/p2.dune_price_ratio as project_a_advantage
    from "query_4334277(start='{{start}}',blockchain='{{blockchain}}',project='{{project_a}}')" p1
    join "query_4334277(start='{{start}}',blockchain='{{blockchain}}',project='{{project_b}}')" p2
        on p1.buy_token = p2.buy_token
        and p1.sell_token = p2.sell_token
    where p1.volume > 0
    and p2.volume > 0
    order by p1.volume desc
    limit {{top_n_tokens}}
)
select 
    -- geometric volume-weighted price ratio
    exp(sum(project_b_volume*ln(project_a_advantage))/sum(project_b_volume)) as avg_project_a_advantage,
    count(project_b_volume) as pairs
from comparison
