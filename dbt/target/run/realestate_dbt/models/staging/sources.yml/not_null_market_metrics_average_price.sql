select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select average_price
from "analytics_db"."public"."market_metrics"
where average_price is null



      
    ) dbt_internal_test