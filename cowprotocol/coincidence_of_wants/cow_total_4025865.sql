-- aggregate volume and cow volume from query 4025739
--
-- Parameters:
--  {{start_time}} - the timestamp for which the analysis should start (inclusively)
--  {{end_time}} - the timestamp for which the analysis should end (exclusively)
--  {{blockchain}} - network to run the analysis on

with t as (
    select
        sum(naive_cow_potential_volume) as naive_cow_potential_volume,
        sum(naive_cow_volume) as naive_cow_volume,
        sum(naive_cow_averaged_volume) as naive_cow_averaged_volume,
        sum(user_out) as total_volume,
        sum(user_in + user_out) as total_volume_in_out
    from "query_4025739(blockchain='{{blockchain}}',start_time='{{start_time}}',end_time='{{end_time}}')"
)
select
    *,
    naive_cow_potential_volume / total_volume as naive_cow_potential_fraction,
    naive_cow_volume / total_volume as naive_cow_fraction,
    naive_cow_averaged_volume / total_volume_in_out as naive_cow_averaged_fraction
from t
