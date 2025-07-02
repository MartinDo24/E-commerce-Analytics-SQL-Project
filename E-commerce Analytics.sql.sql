----Q1----
SELECT 
    format_date('%Y%m',parse_date('%Y%m%d',date)) as month, 
    sum(totals.visits) as visits,
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
 where extract (month from parse_date('%Y%m%d',date)) in (1,2,3) 
 group by month 
 order by month 
 ------Q2-----
 SELECT 
  trafficSource.source,
  count (totals.visits) as totals_visits,
  sum(totals.bounces) as total_no_of_bounces,
  round (count (totals.bounces) / count(totals.visits) *100.0,3) as bounce_rate
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
 group by trafficSource.source
 order by totals_visits desc 
-----Q3-----
select *            
from                  --SUBQUERY ĐỂ ĐẶT ĐIỀU KIỆN THỨ TỰ--
(SELECT                --TÍNH REVENUE THEO THÁNG--
  'Month' AS time_type,
  FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS time_month,
  trafficSource.source,
 sum(product.productRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
unnest(hits) hits,
unnest(hits.product) product
where product.productRevenue is not null
group by trafficSource.source,time_month,time_type

UNION ALL             --GỘP CẢ 2 LẠI 

SELECT                --TÍNH REVENUE THEO TUẦN 
  'week' AS time_type,
  FORMAT_DATE('%Y%W', PARSE_DATE('%Y%m%d', date)) AS time_week,
  trafficSource.source,
 sum(product.productRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`,
unnest(hits) hits,
unnest(hits.product) product
where product.productRevenue is not null
group by trafficSource.source,time_type,time_week)
order by revenue desc 
-----Q4------
with purchase as --Tính purchase  
(SELECT 
count (distinct fullVisitorId) as user,
sum(totals.pageviews) as page_view,
 Format_date ('%Y%m',parse_date('%Y%m%d', date)) as month
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
 unnest(hits) hits,
unnest(hits.product) product
where product.productRevenue is not null
and totals.transactions >=1
and extract(month FROM parse_date('%Y%m%d', date)) in (6,7)
group by month),

non_purchase as --Tính non-purchase   
(SELECT 
count (distinct fullVisitorId) as user,
 sum(totals.pageviews) as page_view,
 Format_date ('%Y%m',parse_date('%Y%m%d', date)) as month
 FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
 unnest(hits) hits,
unnest(hits.product) product
where product.productRevenue is null
and totals.transactions is null 
and extract(month FROM parse_date('%Y%m%d', date)) in (6,7)
group by month)
select   --Tính avg purchase and non-purchase
purchase.month as month,
  (purchase.page_view/purchase.user) as avg_pageviews_purchase,
  (non_purchase.page_view/non_purchase.user) as avg_pageviews_non_purchase
from purchase 
left join non_purchase
on purchase.month=non_purchase.month 
------Q5--------
select 
month, 
trans/user
from
(SELECT  --Tạo số lượng để tính avg lượt giao dịch của mội user vào T7--
  format_date ('%Y%m',parse_date('%Y%m%d', date)) as month,
      count(distinct fullVisitorId ) as user,
      sum(totals.transactions) as trans
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
unnest(hits) hits,
unnest(hits.product) product
where product.productRevenue is not null
and extract(month FROM parse_date('%Y%m%d',date)) =7
group by month)
-----Q6-----
SELECT  
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    (SELECT 
        round((SUM(product.productRevenue) / SUM(totals.visits)) / 1000000,2)
     FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
     UNNEST(hits) hits,
     UNNEST(hits.product) product 
     WHERE product.productRevenue IS NOT NULL
     AND totals.transactions IS NOT NULL) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`  
WHERE EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date)) = 7
GROUP BY month;
----Q7------
with YTB as 
(SELECT   
    fullVisitorId,
    product.v2ProductName as product,
    sum(product.productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) AS hits,
UNNEST(hits.product) AS product
where product.productRevenue is not null 
and  product.v2ProductName="YouTube Men's Vintage Henley"
group by fullVisitorId,product),

other as 
(SELECT  
  fullVisitorId,
   product.v2ProductName as product,
   sum(product.productQuantity) as quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) AS hits,
UNNEST(hits.product) AS product
where product.productRevenue is not null
and   product.v2ProductName NOT IN ("YouTube Men's Vintage Henley")
group by fullVisitorId,product) 
select 
other.product as other_purchased_products,
sum(other.quantity) as quantity  --Tổng thêm lần nữa vì khi nối thì sql không tổng hợp lại quantity
from YTB
left join other
on YTB.fullVisitorId=other.fullVisitorId
group by other.product
order by quantity desc

----Q8-----
with pro as
(SELECT ---Tính num_product_view theo 3 tháng đầu năm---
      format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
      count (eCommerceAction.action_type) as num_product_view,
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST (hits.product) product
    where extract (month from parse_date('%Y%m%d',date)) in (1,2,3)
    and eCommerceAction.action_type='2' 
    group by month,product.productRevenue
    order by month)
,ad as
(SELECT     ---Tính num_addtocart theo 3 tháng đầu năm---
      format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
      count (eCommerceAction.action_type) as num_addtocart,
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST (hits.product) product
    where extract (month from parse_date('%Y%m%d',date)) in (1,2,3)
    and eCommerceAction.action_type='3'
    group by month 
    order by month)
,pur as
(SELECT      ---Tính num_purchase theo 3 tháng đầu năm---
      format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
      count(eCommerceAction.action_type) as num_purchase,
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST(hits) AS hits,
    UNNEST (hits.product) product
    where extract (month from parse_date('%Y%m%d',date)) in (1,2,3)
    and eCommerceAction.action_type='6'
    AND product.productRevenue is not null
    group by month 
    order by month)
 
 select *,
  round((num_addtocart/num_product_view) *100.00,2) as add_to_cart_rate,
  round((num_purchase/num_product_view)*100.00,2) as purchase_rate
FROM
(SELECT   --Gộp các bảng lại để subquery tính toán nốt
    pro.month,
    pro.num_product_view as num_product_view,
    ad.num_addtocart as num_addtocart ,
    pur.num_purchase as num_purchase
FROM pro
LEFT JOIN ad ON pro.month = ad.month
LEFT JOIN pur ON pro.month = pur.month
ORDER BY pro.month)