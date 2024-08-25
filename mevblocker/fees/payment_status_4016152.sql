with
latest_bill_date as (
    select max(call_block_time) as bill_date
    from mev_blocker_ethereum.MevBlockerFeeTill_call_bill
    where call_success = true
),

lastest_billing as (
    select
        usr,
        evt_block_time as bill_time,
        amt as bill_amount
    from mev_blocker_ethereum.MevBlockerFeeTill_evt_Billed
    where evt_block_time = (select bill_date from latest_bill_date)
),

latest_payments as (
    select
        usr,
        evt_block_time as paid_time,
        amt as paid_amount
    from mev_blocker_ethereum.MevBlockerFeeTill_evt_Paid
    where evt_block_time > (select bill_date from latest_bill_date)
),

payment_status as (
    select
        lb.usr,
        bill_amount,
        bill_time,
        paid_time,
        coalesce(paid_amount, 0) as paid_amount
    from lastest_billing as lb
    left outer join latest_payments as lp
        on lb.usr = lp.usr
)

select
    *,
    (case
        when paid_amount = bill_amount then 'PAID'
        when paid_amount < bill_amount then 'UNPAID'
        else 'OVERPAID'
    end) as status
from payment_status
