select 
year(order_date) as "year",
CASE WHEN
    discount >.5 THEN 'Tier 1: 50%+' WHEN
    discount >=.25 THEN 'Tier 2: 25%-50%' WHEN
    discount >0 THEN 'Tier 3: under 25%'ELSE
    'Base Price'
END as discount_tier,
sum(sales) as total_sale,
sum(profit) as total_profit,
round(total_profit/total_sale,2) as profit_margin,
count(row_id) as total_orders
from query_quest.adventure_01.fact_orders 
GROUP BY ALL
ORDER BY "year",total_sale desc

  
