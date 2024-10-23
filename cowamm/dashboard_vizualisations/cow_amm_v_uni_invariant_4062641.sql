select 
    cow.day, cow.volume as cow_volume, uni.volume as uni_volume,
    cow.absolute_invariant_growth as cow_absolute_invariant_growth, uni.absolute_invariant_growth as uni_absolute_invariant_growth,
    cow.tvl as cow_tvl, uni.tvl as uni_tvl, 
    cow.pct_invariant_growth as cow_pct_invariant_growth, uni.pct_invariant_growth as uni_pct_invariant_growth,
    (cow.pct_invariant_growth-uni.pct_invariant_growth)/uni.pct_invariant_growth as performance_difference,
    cow.pct_invariant_growth/uni.pct_invariant_growth as performance_difference_1
from "query_4060136(start='{{start}}', token_a='{{ref_token_a}}', token_b='{{ref_token_b}}', blockchain='{{ref_blockchain}}')" as uni
join "query_4059213(start='{{start}}', token_a='{{token_a}}', token_b='{{token_b}}', blockchain='{{blockchain}}')" as cow
on cow.day=uni.day
order by 1 desc