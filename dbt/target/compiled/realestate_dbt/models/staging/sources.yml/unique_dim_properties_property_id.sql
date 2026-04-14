
    
    

select
    property_id as unique_field,
    count(*) as n_records

from "analytics_db"."public"."dim_properties"
where property_id is not null
group by property_id
having count(*) > 1


