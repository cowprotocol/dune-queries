-- MAKE SURE TO SET THE FORKED QUERY AS PRIVATE AS THIS WILL HOLD SENSITIVE INFORMATION

with
partner_params as (
-- modify as needed
    select 
        0.123 as partner_cut_vol  --ratio of partner volume fee going to integrator (0.0-1.0)
        ,0 as partner_cut_pi  --ratio of partner price improvement fee going to integrator (0.0-1.0), leave as 0 if non-existing
        ,[0x0] as partner_fee_recipient_address  -- if multiple follow syntax: [address_a,addresss_b,etc]
        ,timestamp '2026-01-01' as start_date
)
, prep as (
    select
        t.block_time
        ,t.blockchain
        ,app.app_code
        ,t.usd_value  --uses prices.usd 
        ,if(t.order_type='SELL'
            , t.atoms_bought + coalesce(od.protocol_fee, 0)
            , t.atoms_sold - coalesce(od.protocol_fee, 0)
        ) * coalesce(od.protocol_fee_native_price,0)/1e18 as native_value
        ,if(t.order_type='SELL'
            , t.atoms_bought + coalesce(od.partner_fee, 0)
            , t.atoms_sold - coalesce(od.partner_fee, 0)
        ) as partner_fee_base_amount
        ,t.order_type
        ,t.partial_fill
        ,t.sell_token
        ,t.units_sold
        ,t.buy_token
        ,t.units_bought
        -- fees collected in surplus token        
        ,coalesce(od.partner_fee, 0) as partner_fee  -- raw partner fee, to be split between cow and partner
        ,coalesce(app.partner_bps, 0) as partner_vol_fee_bps
        ,coalesce(od.protocol_fee_native_price, 0) as protocol_fee_native_price
        ,t.buy_token_address
        ,t.sell_token_address
        ,od.solver as solver_address
        ,od.partner_fee_recipient
        ,t.order_uid
        ,t.tx_hash
        ,t.trader
        ,t.app_hash
    from dune.cowprotocol.fct_trades as t
    left join dune.cowprotocol.order_data as od
        on od.order_uid = t.order_uid
        and od.tx_hash = t.tx_hash
    left join dune.cowprotocol.dim_app_data as app
        on app.app_hash = t.app_hash
        and app.blockchain = t.blockchain
    cross join partner_params as pp
    where
        t.block_time >= pp.start_date
        and array_position(pp.partner_fee_recipient_address, od.partner_fee_recipient) > 0
)
-- breakdown of partner fees into volume and price improvement
, partner_fee_breakdown as (
    select
        *
        ,partner_fee_base_amount * partner_vol_fee_bps/1e4 as partner_fee_vol
        ,greatest(0, partner_fee - partner_fee_base_amount * partner_vol_fee_bps/1e4) as partner_fee_pi
    from prep
)
-- split partner fees into cow and partner cuts
, partner_fee_split as (
    select
        p.*
        ,p.partner_fee_vol * pp.partner_cut_vol as partner_vol_fee_partner_cut
        ,p.partner_fee_vol * (1 - pp.partner_cut_vol) as partner_vol_fee_cow_cut
        
        ,p.partner_fee_pi * pp.partner_cut_pi as partner_pi_fee_partner_cut
        ,p.partner_fee_pi * (1 - pp.partner_cut_pi) as partner_pi_fee_cow_cut
        
        ,p.partner_fee_vol * pp.partner_cut_vol + p.partner_fee_pi * pp.partner_cut_pi as partner_fee_partner_cut
        ,p.partner_fee_vol * (1 - pp.partner_cut_vol) + p.partner_fee_pi * (1 - pp.partner_cut_pi) as partner_fee_cow_cut
    from partner_fee_breakdown as p, partner_params as pp
)
, fees_in_native as (
    select
        *
        ,protocol_fee * protocol_fee_native_price/1e18 as protocol_fee_native
        
        ,partner_fee_cow_cut * protocol_fee_native_price/1e18 as partner_fee_cow_cut_native
        ,partner_fee_partner_cut * protocol_fee_native_price/1e18 as partner_fee_partner_cut_native
        
        ,partner_vol_fee_cow_cut * protocol_fee_native_price/1e18 as partner_vol_fee_cow_cut_native
        ,partner_vol_fee_partner_cut * protocol_fee_native_price/1e18 as partner_vol_fee_partner_cut_native
        
        ,partner_pi_fee_cow_cut * protocol_fee_native_price/1e18 as partner_pi_fee_cow_cut_native
        ,partner_pi_fee_partner_cut * protocol_fee_native_price/1e18 as partner_pi_fee_partner_cut_native

    from partner_fee_split
)
, native_token_prices as  (
    select
        p.timestamp 
        ,p.blockchain
        ,p.symbol
        ,p.price
    from prices.hour as p
    join dune.blockchains as b
        on p.blockchain = b.name
        and p.contract_address = b.token_address
    cross join partner_params as pp
    where
        p.timestamp >= pp.start_date
        and p.blockchain in (select distinct blockchain from dune.cowprotocol.fct_trades)
)
, fees_w_usd_values as (
    select
        block_time
        ,f.blockchain
        ,app_code
        ,order_type
        ,partial_fill
        ,coalesce(usd_value, native_value * p_native.price) as usd_value  -- fallback for when Dune's price feed fails
        ,sell_token
        ,buy_token
        ,units_sold
        ,units_bought
        
        ,partner_fee_cow_cut_native           * p_native.price as partner_fee_cow_cut_usd
        ,partner_fee_partner_cut_native       * p_native.price as partner_fee_partner_cut_usd
        ,partner_vol_fee_cow_cut_native       * p_native.price as partner_vol_fee_cow_cut_usd
        ,partner_vol_fee_partner_cut_native   * p_native.price as partner_vol_fee_partner_cut_usd
        ,partner_pi_fee_cow_cut_native        * p_native.price as partner_pi_fee_cow_cut_usd
        ,partner_pi_fee_partner_cut_native    * p_native.price as partner_pi_fee_partner_cut_usd

        ,partner_fee_cow_cut_native
        ,partner_fee_partner_cut_native
        ,partner_vol_fee_cow_cut_native
        ,partner_vol_fee_partner_cut_native
        ,partner_pi_fee_cow_cut_native
        ,partner_pi_fee_partner_cut_native
        
        ,sell_token_address
        ,buy_token_address
        ,solver_address
        ,partner_fee_recipient
        ,order_uid
        ,tx_hash
        ,trader
        ,app_hash
    from 
        fees_in_native as f
    left join native_token_prices as p_native
        on date_trunc('hour',f.block_time) = p_native.timestamp
        and f.blockchain = p_native.blockchain 
)
select *
from fees_w_usd_values
