select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        currency_raw as value_field,
        count(*) as n_records

    from "analytics_db"."staging"."stg_raw_listings"
    group by currency_raw

)

select *
from all_values
where value_field not in (
    'NGN','USD'
)



      
    ) dbt_internal_test