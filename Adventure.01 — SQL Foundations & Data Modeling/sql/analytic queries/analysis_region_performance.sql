
select 
year(order_date) as "year",
l.region,
sum(sales) as total_sales,
sum(profit) as total_profit,
ROUND(sum(profit)/sum(sales),2) as profit_margin,
sum(quantity) as items_ordered,
count(distinct customer_id) as customers,
round(total_sales/customers,2) as sale_per_customer,
from query_quest.adventure_01.fact_orders o
left join query_quest.adventure_01.dim_location l on
city_code = loc_key
GROUP BY ALL
order by "year", total_sales desc
