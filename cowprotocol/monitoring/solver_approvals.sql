with ranked as (
    select
        evt_tx_hash as tx_hash,
        evt_block_time as block_time,
        "from" as responsible_address,
        spender,
        contract_address as token,
        a.value,
        rank() over (partition by spender, contract_address order by evt_block_number desc, evt_index desc) as rk
    from erc20_{{blockchain}}.evt_approval as a
    inner join {{blockchain}}.transactions on evt_tx_hash = hash
    where owner = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
),
solvers as (
    select
        address,
        environment,
        name,
        whitelisted as active
    from dune.cowprotocol.solvers
    where blockchain = '{{blockchain}}'
)
select --noqa: ST06
    r.block_time,
    r.tx_hash,
    r.responsible_address,
    case
        when r.responsible_address = 0x05c5494572e4ab2d48d3ab3aaf6bd4e7b1c98382 or r.responsible_address = 0xd8ca5fe380b68171155c7069b8df166db28befdd then 'PROPOSER-ACCOUNT'
        else coalesce(concat(s.environment, '-', s.name), 'NON-SOLVER')
    end as responsible_solver,
    r.spender,
    r.token,
    r.value
from ranked as r
inner join cow_protocol_{{blockchain}}.batches as b on r.tx_hash = b.tx_hash
left outer join solvers as s on b.solver_address = s.address
where r.rk = 1 and r.value > 0
order by r.block_time desc
