-- Base query to convert accounting period to block range.
-- Convention throughout the accounting that is used is that we consider all auctions whose
-- block deadline is in the block range this query returns
-- Parameters:
--  {{start_time}} - the start date timestamp for the accounting period  (inclusively)
--  {{end_time}} - the end date timestamp for the accounting period (exclusively)
-- {{blockchain}} -- the corresponding chain

select
    min("number") as start_block,
    max("number") as end_block
from {{blockchain}}.blocks
where time >= cast('{{start_time}}' as timestamp) and time < cast('{{end_time}}' as timestamp)
