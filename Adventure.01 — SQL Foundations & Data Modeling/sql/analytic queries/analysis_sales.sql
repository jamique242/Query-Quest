WITH sales AS 
(
SELECT distinct
year(order_date) as "year",
month(order_date) as period,
sum(sales) as total_sales,
sum(profit) as total_profit,
Round(total_profit/total_sales,2) as profit_margin
FROM query_quest.adventure_01.fact_orders GROUP BY ALL
)
select *,
sum(total_sales) OVER (PARTITION BY "year" ORDER BY period) as ytd_sales,
sum(total_profit) OVER (PARTITION BY "year" ORDER BY period) as ytd_profit,
round(ytd_profit/ytd_sales,2) as ytd_profit_margin
from sales
ORDER by Year,Period
