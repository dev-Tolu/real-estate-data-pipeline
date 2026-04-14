select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select source_site
from "analytics_db"."staging"."stg_listings"
where source_site is null



      
    ) dbt_internal_test