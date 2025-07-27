# E-commerce-Analytics SQL Project - Visitor Behavior & Conversion Funnel
<img width="1024" height="1024" alt="image" src="https://github.com/user-attachments/assets/358e12da-810e-4b35-a892-0260be8e7c4b" />

Author: Đỗ Hoàng Minh

Date: 2025-04-25

Tools Used: SQL (BigQuery)

##📑 Table of Contents:

1.[📌Background & Overview](#-background--overview)

2.[📂 Dataset Description & Data Structure](#-dataset-description--data-structure)

3.[🔎 Final Conclusion & Recommendations](#-final-conclusion--recommendations)


## 📌 Background & Overview

###Objective:

###📖 What is this project about? What Business Question will it solve?

 ✅ This project uses SQL to analyze ecommerce website data, including traffic, user sessions, and transactions.
 
 ✅ It transforms raw data into insights on user behavior, purchase patterns, and engagement.
 
 ✅ Which traffic sources drive the most visits, revenue, and conversions
 
 ✅How do users move through the conversion funnel from product view to purchase


### 👤 Who is this project for? 

-Decision-makers & ecommerce stakeholders

-Data analysts & business analysts

## 📂 Dataset Description & Data Structure

- Source: Public dataset from [google_analytics_sample – BigQuery] https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1sbigquery-public-data!2sgoogle_analytics_sample!3sga_sessions_20170801

- Size: Over 1 million rows across multiple tables (Sessions, Products, Transactions, etc.)

- Format : BigQuery (cloud SQL-based format)

### 📊 Data Structure & Relationships

#### 1️⃣ Tables Used: 

-Total Tables Used: 4

-Main Table: ga_sessions_2017*

-Nested Tables (via UNNEST):

+ hits

+ hits.product

+ hits.eCommerceAction

#### 2️⃣ Table Schema & Data Snapshot :

Table : ga_sessions_2017* (from BigQuery public dataset)

| Column Name                       | Data Type | Description                                    |
|----------------------------------|-----------|-------------------------------------------------|
| fullVisitorId                    | STRING    | Unique identifier for each user                 |
| date                             | STRING    | Session date in YYYYMMDD format                 |
| totals.visits                    | INTEGER   | Number of visits in the session                 |
| totals.pageviews                 | INTEGER   | Number of pageviews in the session              |
| totals.transactions              | INTEGER   | Number of transactions in the session           |
| totals.bounces                   | INTEGER   | 1 if bounced, NULL otherwise                    |
| trafficSource.source             | STRING    | Source of the session traffic                   |
| hits.eCommerceAction.action_type | STRING    | Ecommerce action: 2=view, 3=add-to-cart, 6=purchase |
| hits.product.v2ProductName       | STRING    | Product name                                    |
| hits.product.productQuantity     | INTEGER   | Quantity of the product purchased               |
| hits.product.productRevenue      | INTEGER   | Product revenue in micros (÷1,000,000 for USD)  |


## ⚒️ Main Process :

1️⃣ Data Cleaning & Preprocessing

-Filtered sessions from relevant time periods (Jan–Jul 2017).

-Removed null values in key metrics like transactions, productRevenue, and bounces.

-Used UNNEST() to flatten nested fields (hits, product, eCommerceAction).

2️⃣ Exploratory Data Analysis (EDA)

-Aggregated key metrics such as total visits, pageviews, transactions, and revenue.

-Compared user behaviors (e.g., purchasers vs. non-purchasers).

-Calculated conversion funnel rates and bounce rate per traffic source

3️⃣ SQL Analysis

-Wrote 8 SQL queries to answer key business questions.

-Used JOIN, GROUP BY, CASE, WITH CTE, and conditional filters.

-Extracted actionable metrics such as bounce rate, conversion funnel, revenue by source, etc.

## Task 1: Calculate total visit, pageview, transaction for Jan, Feb and March 2017 

-Queried the ga_sessions_2017* table.

-Used FORMAT_DATE() and GROUP BY month to aggregate visit, pageview, and transaction data.

-Order by month

<img width="651" height="109" alt="image" src="https://github.com/user-attachments/assets/5d05f130-89d1-4d46-995b-e356b42bd5fc" />

## Task 2: Bounce rate per traffic source in July 2017

-Bounce_rate = num_bounce/total_visit

-Used COUNT(totals.visits) and SUM(totals.bounces) to compute bounce rate by source

-Grouped by trafficSource.source, ordered by number of visits in descending order

<img width="643" height="246" alt="image" src="https://github.com/user-attachments/assets/d2b0cf70-2805-435a-a0e5-ced318529561" />

<img width="630" height="238" alt="image" src="https://github.com/user-attachments/assets/ba4e642a-5ae3-4f62-a15c-6e4494e08031" />

## Task 3: Revenue by traffic source by week, by month in June 2017

-Accessed hits.product.productRevenue using UNNEST(hits) and UNNEST(product)

-Used FORMAT_DATE() to group by week and month

-Combined both weekly and monthly outputs using UNION ALL

<img width="785" height="235" alt="image" src="https://github.com/user-attachments/assets/08efe326-3820-4436-92b1-35650375c38e" />

<img width="784" height="237" alt="image" src="https://github.com/user-attachments/assets/ba74316e-395b-4f9e-836a-0466ab25e71f" />

## Task 4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.

-Created two CTEs: one for purchasers (productRevenue IS NOT NULL) and one for non-purchasers (transactions IS NULL).

-Calculated SUM(pageviews) / COUNT(DISTINCT fullVisitorId) for both types.

-Joined both CTEs to compare.

<img width="509" height="74" alt="image" src="https://github.com/user-attachments/assets/897694af-489b-43c3-a3d2-f87f63225284" />

## Task 5:  Average number of transactions per user that made a purchase in July 2017

-Filtered sessions with purchases.

-Calculated total transactions and number of unique purchasing users.

-Divided transactions / users.

<img width="385" height="48" alt="image" src="https://github.com/user-attachments/assets/d7a5db63-6d37-49a9-b277-8752a923e89a" />

## Task 6: : Average amount of money spent per session. Only include purchaser data in July 2017

-Filtered sessions where productRevenue IS NOT NULL.

-Divided total revenue by total visits within July.

-Used a subquery to simplify calculation and return 1 row per month.

<img width="384" height="50" alt="image" src="https://github.com/user-attachments/assets/d6348fa6-ea02-416a-b71b-4443ce32ea55" />

## Task 7:  Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.

-Created two CTEs:

   + One to get all users who purchased the target product.
   
   + One to list other products those users purchased.
-Joined both on fullVisitorId and aggregated quantity by product name.

<img width="386" height="249" alt="image" src="https://github.com/user-attachments/assets/fc3c127b-b51e-4acd-be23-d06e1f165ab4" />

<img width="385" height="247" alt="image" src="https://github.com/user-attachments/assets/31f2d367-3e8a-4796-95b3-4fe17fbae262" />

## Task 8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. 

-Created three CTEs: one for product views, one for add-to-cart, one for purchases.

-Each filtered by eCommerceAction.action_type = 2 / 3 / 6.(hits.eCommerceAction.action_type = '2' is view product page; hits.eCommerceAction.action_type = '3' is add to cart; hits.eCommerceAction.action_type = '6' is purchase)

-Joined all three by month and calculated:
  
  + add_to_cart_rate = num_add_to_cart / num_product_view
  
  + purchase_rate = num_purchase / num_product_view

<img width="898" height="104" alt="image" src="https://github.com/user-attachments/assets/f62ed06a-32cf-4dc0-a8f5-4f3d58d40a77" />

## 🔎 Final Conclusion & Recommendations

Based on the insights and findings above, we would recommend the Ecommerce & Marketing team to consider the following:

📌 Key Takeaways:

-[Q1] Traffic & Transactions Trend (Jan–Mar 2017):
Website experienced stable traffic volume across Q1, but transactions did not increase proportionally, suggesting potential drop-offs or low conversion.

-[Q2] Bounce Rate by Source (Jul 2017):
Some traffic sources like partners and bing had extremely high bounce rates (>70%), indicating poor landing page experience or irrelevant targeting.

-[Q3] Revenue by Source (Jun 2017):
Google and direct traffic brought in the highest revenue. However, weekly trends showed fluctuations—pointing to possible inconsistency in ad or campaign performance.

-[Q4] Pageviews by User Type:
Purchasers viewed significantly more pages per session than non-purchasers. This suggests that deeper engagement leads to higher conversion.

-[Q5] Avg. Transactions per Purchaser (Jul 2017):
Most users made only one transaction, indicating that repeat purchases are low. Retention or loyalty campaigns could be considered.

-[Q6] Avg. Revenue per Session (Jul 2017):
The average amount spent per session among purchasers was meaningful but left room for upselling or bundle strategies.

-[Q7] Co-purchased Products with “Henley”:
Users who purchased “YouTube Men’s Vintage Henley” also bought several related items. This presents an opportunity for bundling or personalized recommendation.

-[Q8] Funnel Drop-offs (Jan–Mar 2017):
While many products were viewed, only a small portion reached “add to cart” and even fewer were purchased. Funnel conversion rates were low (<15%), especially at the purchase stage


