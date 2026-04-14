select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select listing_date
from "analytics_db"."public"."market_metrics"
where listing_date is null



      
    ) dbt_internal_test