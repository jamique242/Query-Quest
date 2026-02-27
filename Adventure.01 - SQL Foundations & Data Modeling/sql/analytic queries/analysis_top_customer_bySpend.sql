with customer_orders as (
  select 
    year(order_date) as "year",
    o.customer_id,
    c.customer_name,
    sum(sales) as total_sales,
    sum(quantity) as total_quantity
  from query_quest.adventure_01.fact_orders o
  LEFT JOIN query_quest.adventure_01.dim_customer c ON
  o.customer_id = c.customer_id
  group by all
)

select *,
rank() over (order by total_sales desc) as total_rank,
from (select *, rank() over (partition by "year" order by total_sales desc) as year_rank from customer_orders)
where year_rank <= 10
order by "year",year_rank asc
