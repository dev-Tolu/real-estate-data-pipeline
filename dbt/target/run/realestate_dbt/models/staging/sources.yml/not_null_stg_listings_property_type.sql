select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select property_type
from "analytics_db"."staging"."stg_listings"
where property_type is null



      
    ) dbt_internal_test