select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select address
from "analytics_db"."public"."properties"
where address is null



      
    ) dbt_internal_test