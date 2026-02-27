/*
Create or update dim location from staging table ... a pk was created as we found multiple cities
to 1 postal code. dim location allows us to have an independent table for shipping location at a grain level
*/

CREATE OR REPLACE TABLE query_quest.adventure_01.dim_location 
AS
SELECT distinct
city,
state,
postal_code,
country,
region,
postal_code ||'-'||city as loc_key
from query_quest.adventure_01.stg_orders
