
    
    

with all_values as (

    select
        source_site as value_field,
        count(*) as n_records

    from "analytics_db"."staging"."stg_raw_listings"
    group by source_site

)

select *
from all_values
where value_field not in (
    'PropertyPro','NigeriaPropertyCentre','PrivateProperty','Unknown'
)


