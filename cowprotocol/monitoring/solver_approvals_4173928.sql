with
ranked as (
    select
        evt_tx_hash as tx_hash,
        evt_block_time as block_time,
        "from" as tx_sender,
        spender,
        contract_address as token,
        a.value,
        rank() over (partition by spender, contract_address order by evt_block_number desc, evt_index desc) as rk
    from erc20_ethereum.evt_Approval as a
    left outer join ethereum.transactions on evt_tx_hash = hash and "to" = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
    where owner = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
)

select --noqa: ST06
    tx_hash,
    block_time,
    coalesce(cast(tx_sender as varchar), 'NON-SOLVER') as responsible_solver,
    spender,
    token,
    value
from ranked
where rk = 1 and value > 0
order by block_time desc
