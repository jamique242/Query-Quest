CREATE OR REPLACE TABLE query_quest.adventure_01.dim_customer
AS
select distinct 
customer_id,
customer_name,
segment
from query_quest.adventure_01.stg_orders
