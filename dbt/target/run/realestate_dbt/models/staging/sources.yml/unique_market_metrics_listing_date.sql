select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    listing_date as unique_field,
    count(*) as n_records

from "analytics_db"."public"."market_metrics"
where listing_date is not null
group by listing_date
having count(*) > 1



      
    ) dbt_internal_test