{{ config(materialized='table' )}}  -- make sql in 'core' as tables since these are exposed to BI tools. Tables being more efficient

select
    locationid,
    borough,
    zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
from {{ ref('txi_zone_lookup') }}
