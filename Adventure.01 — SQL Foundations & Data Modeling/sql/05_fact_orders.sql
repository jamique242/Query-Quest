/*
Create fact orders table with relevant secondary keys for joins
*/
CREATE OR REPLACE TABLE query_quest.adventure_01.fact_orders
AS
select 
row_id,
order_id,
order_date,
ship_date,
ship_mode,customer_id,
postal_code||'-'||city as city_code,
product_id,
sales,quantity,
discount,
profit
from query_quest.adventure_01.stg_orders 
