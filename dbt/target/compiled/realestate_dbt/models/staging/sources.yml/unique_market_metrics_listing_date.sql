
    
    

select
    listing_date as unique_field,
    count(*) as n_records

from "analytics_db"."public"."market_metrics"
where listing_date is not null
group by listing_date
having count(*) > 1


