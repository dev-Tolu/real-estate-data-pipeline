
    
    

select
    property_id as unique_field,
    count(*) as n_records

from "analytics_db"."staging"."stg_listings"
where property_id is not null
group by property_id
having count(*) > 1


