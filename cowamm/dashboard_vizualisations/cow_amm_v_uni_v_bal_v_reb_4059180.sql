-- plots the different strategies against each other

select 
    cow.day, 
    cow.current_value_of_investment as "CoW AMM",
    uni.current_value_of_investment as "Uni v2",
    bal.current_value_of_investment as "Balancer",
    reb.current_value_of_investment as "Daily Rebalancing",
    hodl.current_value_of_investment as "HODL"
from "query_4047078(start='{{start}}', token_a='{{token_a}}', token_b='{{token_b}}', blockchain='{{blockchain}}')" cow
left join "query_4047194(start='{{start}}', token_a='{{ref_token_a}}', token_b='{{ref_token_b}}', blockchain='{{ref_blockchain}}')" uni
on cow.day=uni.day
left join "query_4106553(start='{{start}}', token_a='{{ref_token_a}}', token_b='{{ref_token_b}}', blockchain='{{ref_blockchain}}')" bal
on cow.day=bal.day
left join "query_4055484(start='{{start}}', token_a='{{ref_token_a}}', token_b='{{ref_token_b}}')" reb
on cow.day=reb.day
left join "query_4086902(start='{{start}}', token_a='{{ref_token_a}}', token_b='{{ref_token_b}}')" hodl
on cow.day=hodl.day
where cow.day< date(now())
order by cow.day desc