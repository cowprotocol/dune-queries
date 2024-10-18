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
    inner join ethereum.transactions on evt_tx_hash = hash
    where owner = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
)

select --noqa: ST06
    block_time,
    tx_hash,
    tx_sender as responsible_address,
    coalesce(concat(environment, '-', name), 'NON-SOLVER') as responsible_solver,
    spender,
    token,
    value
from ranked as r
left outer join cow_protocol_ethereum.solvers as s on r.tx_sender = s.address
where rk = 1 and value > 0
order by block_time desc
