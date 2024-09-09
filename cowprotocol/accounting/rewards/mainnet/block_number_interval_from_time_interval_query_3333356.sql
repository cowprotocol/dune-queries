select
    min("number") as start_block,
    max("number") as end_block
from ethereum.blocks
where time >= cast('{{start_time}}' as timestamp) and time < cast('{{end_time}}' as timestamp)
