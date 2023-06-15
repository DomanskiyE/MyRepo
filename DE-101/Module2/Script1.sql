-- Monthly Sales by Product Category
with f as (select o.category,date_trunc('month', o.order_date) as date, sum(o.sales) over (partition by o.category, date_trunc('month', o.order_date)) as total_sales
from orders o)
select f.category, to_char(f.date, 'Month YYYY') as date, f.total_sales
from f
group by f.category, f.date, f.total_sales
order by f.date desc, total_sales

-- Profit Ratio
select round(100*sum(o.profit)/sum(o.sales))::text || '%' as profit_ratio
from orders o 

-- Customer Ranking
with f as (select customer_id, sum(sales) over (partition by customer_id) as sales_by_customer
from orders)
select customer_id, sales_by_customer, dense_rank() over (order by sales_by_customer desc) as rank
from f
group by customer_id, sales_by_customer
