select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select inventory_count
from "analytics_db"."public"."market_metrics"
where inventory_count is null



      
    ) dbt_internal_test