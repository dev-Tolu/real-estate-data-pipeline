select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select property_id
from "analytics_db"."staging"."stg_listings"
where property_id is null



      
    ) dbt_internal_test