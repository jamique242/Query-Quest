/*
Multiple product names were found mapped to a single product id , this seemed sporadic and we chose the name with the highest count for each product_id
*/
CREATE OR REPLACE TABLE query_quest.adventure_01.dim_products
AS
SELECT 
    product_id,
    product_name,
    category,
    sub_category
FROM query_quest.adventure_01.stg_orders
GROUP BY product_id, product_name, category, sub_category
QUALIFY ROW_NUMBER() 
          OVER (
                  PARTITION BY product_id
                  ORDER BY COUNT(*) DESC
                ) = 1
