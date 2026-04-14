select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select raw_price_numeric
from "analytics_db"."staging"."stg_listings"
where raw_price_numeric is null



      
    ) dbt_internal_test