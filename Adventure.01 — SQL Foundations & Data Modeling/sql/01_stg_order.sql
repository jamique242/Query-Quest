--Query creates or updates main staging table

CREATE OR REPLACE TABLE 
query_quest.adventure_01.stg_orders
AS
SELECT 
CAST("Row ID" AS INT)                AS row_id,
CAST("Order ID" AS VARCHAR)             AS order_id, 	
CAST("Order Date"	AS DATE)              AS order_date,
CAST("Ship Date" AS	DATE)               AS ship_date,			
CAST("Ship Mode" AS VARCHAR)            AS ship_mode,
CAST("Customer ID" AS VARCHAR)          AS customer_id,
CAST("Customer Name" AS VARCHAR)        AS customer_name,
CAST("Segment" AS VARCHAR)              AS segment,
CAST("Country" AS VARCHAR)              AS country,
CAST("City" AS VARCHAR)                 AS city,
CAST("State" AS VARCHAR)                AS state,
LEFT(CAST("Postal Code" AS VARCHAR),5)  AS postal_code,
CAST("Region" AS VARCHAR)               AS region,
CAST("Product ID" AS VARCHAR)           AS product_id,
CAST("Category" AS VARCHAR)             AS category,
CAST("Sub-Category" AS VARCHAR)         AS sub_category,
CAST("Product Name" AS VARCHAR)         AS product_name,
CAST("Sales" AS DECIMAL(12,2))          AS sales,
CAST("Quantity" AS INT)                 AS quantity,
CAST("Discount" AS DECIMAL(5,4))        AS discount,
CAST("Profit" AS DECIMAL(12,2))         AS profit
FROM 
query_quest.adventure_01.super_store
WHERE "Row ID" is not null
