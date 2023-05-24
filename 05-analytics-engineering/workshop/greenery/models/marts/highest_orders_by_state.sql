with

order_state as (
    select orders.order_guid
        , orders.address_guid
        , addr.state
    from {{ ref('stg_greenery__orders' )}} as orders
    inner join {{ ref('stg_greenery__addresses' )}} as addr
    on orders.address_guid=addr.address_guid
)

, final as (
    select state, count(*) as state_count
    from order_state
    group by state
    order by state_count desc
    limit 1
)

select * from final