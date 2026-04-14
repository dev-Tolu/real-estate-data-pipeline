select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price_range
from "analytics_db"."public"."fct_price_history"
where price_range is null



      
    ) dbt_internal_test