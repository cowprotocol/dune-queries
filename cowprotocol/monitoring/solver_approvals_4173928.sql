with
ranked as (
    select
        evt_tx_hash as tx_hash,
        evt_block_time as block_time,
        "from" as responsible_address,
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
    responsible_address,
    case
        when responsible_address = 0x05C5494572E4aB2d48D3AB3aAF6bD4e7b1c98382 then 'PROPOSER-ACCOUNT'
        else coalesce(concat(environment, '-', name), 'NON-SOLVER')
    end as responsible_solver,
    spender,
    token,
    value
from ranked as r
left outer join cow_protocol_ethereum.solvers as s on r.responsible_address = s.address
where rk = 1 and value > 0 and responsible_address != 0x05C5494572E4aB2d48D3AB3aAF6bD4e7b1c98382 -- exclude proposer account
order by block_time desc
