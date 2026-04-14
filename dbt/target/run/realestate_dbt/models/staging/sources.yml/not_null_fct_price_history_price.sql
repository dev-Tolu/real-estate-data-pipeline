select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price
from "analytics_db"."public"."fct_price_history"
where price is null



      
    ) dbt_internal_test