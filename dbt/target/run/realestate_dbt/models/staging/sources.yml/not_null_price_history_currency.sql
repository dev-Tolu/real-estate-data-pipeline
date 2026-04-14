select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select currency
from "analytics_db"."public"."price_history"
where currency is null



      
    ) dbt_internal_test