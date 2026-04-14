select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        price_range as value_field,
        count(*) as n_records

    from "analytics_db"."public"."fct_price_history"
    group by price_range

)

select *
from all_values
where value_field not in (
    'Budget','Mid-Range','High-End','Luxury'
)



      
    ) dbt_internal_test