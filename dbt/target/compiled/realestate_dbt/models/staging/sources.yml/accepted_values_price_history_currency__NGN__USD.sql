
    
    

with all_values as (

    select
        currency as value_field,
        count(*) as n_records

    from "analytics_db"."public"."price_history"
    group by currency

)

select *
from all_values
where value_field not in (
    'NGN','USD'
)


