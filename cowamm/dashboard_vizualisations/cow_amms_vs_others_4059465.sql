with prep as (
select 
    cow.day, 
    cow.current_value_of_investment as cow_current_value_of_investment,
    uni.current_value_of_investment as uni_current_value_of_investment,
    bal.current_value_of_investment as bal_current_value_of_investment,
    uni.tvl as uni_tvl,
    bal.tvl as bal_tvl,
    reb.current_value_of_investment as rebalanced_current_value_of_investment
from "query_4047078(start='{{start}}', token_a='{{token_a}}', token_b='{{token_b}}', blockchain='{{blockchain}}')" cow
left join "query_4047194(start='{{start}}', token_a='{{ref_token_a}}', token_b='{{ref_token_b}}', blockchain='{{ref_blockchain}}')" uni
on cow.day=uni.day
left join "query_4106553(start='{{start}}', token_a='{{ref_token_a}}', token_b='{{ref_token_b}}', blockchain='{{ref_blockchain}}')" bal
on cow.day=bal.day
left join "query_4055484(start='{{start}}', token_a='{{ref_token_a}}', token_b='{{ref_token_b}}')" reb
on cow.day=reb.day
order by cow.day desc
limit 1
)


select 
    ((cow_current_value_of_investment/uni_current_value_of_investment)-1)*100 as over_uni_return,
    ((cow_current_value_of_investment/bal_current_value_of_investment)-1)*100 as over_bal_return,
    ((cow_current_value_of_investment/rebalanced_current_value_of_investment)-1)*100 as over_reb_return,
    uni_tvl,
    bal_tvl
from prep
