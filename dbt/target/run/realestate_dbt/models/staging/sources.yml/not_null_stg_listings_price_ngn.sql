select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price_ngn
from "analytics_db"."staging"."stg_listings"
where price_ngn is null



      
    ) dbt_internal_test