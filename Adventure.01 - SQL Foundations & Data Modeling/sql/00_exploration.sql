/*
These sql queries can be used to generally explore the dataset and become more aquainted with your data
multiple lines have been comented out but are there to record some examples used
*/

SELECT *
--COUNT(*) --count all rows: How many rows?
--COUNT(distinct "Order ID")--COUNT all unique order IDs: How many orders?
--COUNT(distinct "Product ID")--Count all unique products sold: How many products?
--COUNT(distinct "Customer ID")--How many unique customers?
--min("Order Date")--earliest date of entry? similar for max() - latest
FROM query_quest.adventure_01.super_store
--WHERE "Order ID" is null --Any null order entries?
--WHERE "Product ID" is null -- Any null products?
--WHERE "Customer ID" is null -- Any null products?
--WHERE "Order Date" is null --Null date check
--WHERE "Order ID" = 'CA-2016-152156' --investigates single order to learn more about grain

/*
This query was used to quickly validate my theory data being order-id, product-id grain.
If any value is returned the theory is invalidated. If duplicates found look into those grouped values

--looks into individual duplicate to find true grain
-- FROM query_quest.adventure_01.super_store
-- WHERE "Order ID" || "Product ID" = 'CA-2017-152912OFF-ST-10003208'
*/
  
SELECT
"Order ID" || "Product ID" as Order_key,
COUNT(*) as total_rows
FROM query_quest.adventure_01.super_store
GROUP BY Order_key
HAVING total_rows > 1
ORDER BY total_rows DESC

/*
After research I found an alternative to testing these theories.
It is a lot more straightfoward and can be useful as another sanity check 
*/
-- count all  distinct order & product id pairs vs count of all rows from is commented out to allow easy swapping between sources
select 
count(*)
from 
-- query_quest.adventure_01.super_store
(select distinct  "Order ID", "Product ID" from query_quest.adventure_01.super_store)
