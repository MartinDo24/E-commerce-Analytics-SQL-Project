# E-commerce-User-Behavior-Analyst-SQL-Project

<img width="480" height="360" alt="image" src="https://github.com/user-attachments/assets/72b175e6-ec15-4a9f-86f8-cd6026e474d2" />

Author: Đỗ Hoàng Minh

Date: 2025-04-25

Tools Used: SQL (BigQuery)

##📑 Table of Contents:

1.[📌Background & Overview](#-background--overview)

2.[📂 Dataset Description & Data Structure](#-dataset-description--data-structure)

3.[🔎 Final ](#-final-conclusion--recommendations)


## 📌 Background & Overview

### Objective:

### 📖 What is this project about? What Business Question will it solve?

 ✅ This project uses SQL to analyze ecommerce website data, including traffic, user sessions, and transactions.
 
 ✅ It transforms raw data into insights on user behavior, purchase patterns, and engagement.
 
 ✅ Which traffic sources drive the most visits, revenue, and conversions
 
 ✅How do users move through the conversion funnel from product view to purchase


### 👤 Who is this project for? 

-Decision-makers & ecommerce stakeholders looking to improve traffic efficiency and sales performance.

-Data analysts & business analysts want to analyze user behavior and conversion metrics

## 📂 Dataset Description & Data Structure

- Source: Public dataset from [google_analytics_sample – BigQuery] https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1sbigquery-public-data!2sgoogle_analytics_sample!3sga_sessions_20170801

- Size: Over 1 million rows across multiple tables (Sessions, Products, Transactions, etc.)

- Format : E-commerce-Analytics.sql

### 📊 Data Structure & Relationships

#### 1️⃣ Tables Used: 

-Main Table: ga_sessions_2017*

-Table used:

+ totals

+ product

+ fullVisitorId

+ hits

#### 2️⃣ Table Schema & Data Snapshot

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

-Traffic & Transaction Trends: Help stakeholders assess whether marketing efforts are driving consistent traffic and if that traffic is converting to revenue.

<img width="519" height="153" alt="image" src="https://github.com/user-attachments/assets/fded5980-6e89-4af4-bc71-28ea1573c752" />


<img width="651" height="109" alt="image" src="https://github.com/user-attachments/assets/5d05f130-89d1-4d46-995b-e356b42bd5fc" />

[Q1] Traffic & Transactions Trend (Jan–Mar 2017):

Website traffic remained relatively stable across Q1 2017, with visits and pageviews peaking in March.

Transactions rose significantly in March (+35% vs February), indicating an improvement in conversion efficiency despite modest traffic growth.

## Task 2: Bounce rate per traffic source in July 2017

-Bounce rate measures the percentage of sessions where users landed on the website but left without interacting (e.g., viewing only one page).
A high bounce rate can indicate poor landing page relevance, weak content, or targeting the wrong audience.

-Bounce_rate = num_bounce/total_visit

-Used COUNT(totals.visits) and SUM(totals.bounces) to compute bounce rate by source

-Grouped by trafficSource.source, ordered by number of visits in descending order

<img width="613" height="139" alt="image" src="https://github.com/user-attachments/assets/5341b990-a3c6-40a9-ae5d-5984b7adba2b" />


<img width="643" height="246" alt="image" src="https://github.com/user-attachments/assets/d2b0cf70-2805-435a-a0e5-ced318529561" />

<img width="630" height="238" alt="image" src="https://github.com/user-attachments/assets/ba4e642a-5ae3-4f62-a15c-6e4494e08031" />

[Q2] Bounce Rate by Source (Jul 2017):
Google and Direct brought the most traffic with moderate bounce rates (~51% and 43%).

YouTube had high traffic but a high bounce rate (66.7%) → low engagement.

Several sources like DuckDuckGo, Ask, and productforums.google.com had very high bounce rates (>80%) → likely poor traffic quality or irrelevant landing pages.

Reddit and Mail traffic showed strong engagement with low bounce rates (<30%).


## Task 3: Revenue by traffic source by week, by month in June 2017

-Accessed hits.product.productRevenue using UNNEST(hits) and UNNEST(product)

-Used FORMAT_DATE() to group by week and month

-Combined both weekly and monthly outputs using UNION ALL

<img width="529" height="419" alt="image" src="https://github.com/user-attachments/assets/7793b00e-2646-45de-8186-81c7527ef77a" />

<img width="785" height="235" alt="image" src="https://github.com/user-attachments/assets/08efe326-3820-4436-92b1-35650375c38e" />

<img width="784" height="237" alt="image" src="https://github.com/user-attachments/assets/ba74316e-395b-4f9e-836a-0466ab25e71f" />

[Q3] Revenue by Source (Jun 2017):

(direct) traffic was the dominant source, generating over $97,000 in June, accounting for the majority of weekly revenue.

Google was the second-highest source (~$18,757), showing consistent but lower contribution.

Other sources like dfa and mail.google.com contributed moderately, with occasional weekly spikes.

Long-tail sources (e.g., bing, youtube.com, dealspotr.com) generated minimal revenue, showing limited commercial impact.


## Task 4: Average number of pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017.

-Created two CTEs: one for purchasers (productRevenue IS NOT NULL) and one for non-purchasers (transactions IS NULL).

-Calculated SUM(pageviews) / COUNT(DISTINCT fullVisitorId) for both types.

-Joined both CTEs to compare.


<img width="547" height="521" alt="image" src="https://github.com/user-attachments/assets/88aa7ea4-c60f-4252-94cd-c8a48a179931" />


<img width="509" height="74" alt="image" src="https://github.com/user-attachments/assets/897694af-489b-43c3-a3d2-f87f63225284" />

[Q4] Pageviews by User Type:

Non-purchasers viewed 3x more pages than purchasers in both months.

This suggests users are actively browsing but not converting, indicating possible issues in product offering, pricing, or checkout flow.

## Task 5:  Average number of transactions per user that made a purchase in July 2017

-Filtered sessions with purchases.

-Calculated total transactions and number of unique purchasing users.

-Divided transactions / users.


<img width="526" height="227" alt="image" src="https://github.com/user-attachments/assets/fc5013ca-29f7-4e82-9a62-6058f9750012" />

<img width="385" height="48" alt="image" src="https://github.com/user-attachments/assets/d7a5db63-6d37-49a9-b277-8752a923e89a" />

[Q5] Avg. Transactions per Purchaser (Jul 2017):

Each purchaser made 4.16 transactions on average in July 2017.

This reflects strong purchase intent and potentially high-value users — indicating an opportunity to build loyalty and retention programs around these buyers.



## Task 6: : Average amount of money spent per session. Only include purchaser data in July 2017

-Filtered sessions where productRevenue IS NOT NULL.

-Divided total revenue by total visits within July.

-Used a subquery to simplify calculation and return 1 row per month.

- This metric indicates how much revenue is generated on average per session from users who actually made purchases.

-It helps assess the monetary value of each converting session, which is useful for budgeting paid traffic, setting CPA goals, or forecasting revenue.

<img width="567" height="197" alt="image" src="https://github.com/user-attachments/assets/052f8498-7c68-4e14-aa5b-c78af18a9a6b" />

<img width="384" height="50" alt="image" src="https://github.com/user-attachments/assets/d6348fa6-ea02-416a-b71b-4443ce32ea55" />

[Q6] Avg. Revenue per Session (Jul 2017):

On average, each purchasing session generated $43.86 in July 2017.

This shows strong monetary value per session, which can guide ad spend limits (CPA/CPC), campaign targeting, and product bundling strategies.

## Task 7:  Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.

-Created two CTEs:

   + One to get all users who purchased the target product.
   
   + One to list other products those users purchased.

-Joined both on fullVisitorId and aggregated quantity by product name.

<img width="741" height="503" alt="image" src="https://github.com/user-attachments/assets/22b31a8d-eee5-47ab-af16-b35b61bf2174" />

<img width="386" height="249" alt="image" src="https://github.com/user-attachments/assets/fc3c127b-b51e-4acd-be23-d06e1f165ab4" />

<img width="385" height="247" alt="image" src="https://github.com/user-attachments/assets/31f2d367-3e8a-4796-95b3-4fe17fbae262" />

[Q7] Co-purchased Products with “Henley”:

-Google Sunglasses and hero-themed apparel were most frequently purchased with the YouTube Men’s Vintage Henley.

-This reveals strong product affinity and bundling behavior, suggesting opportunities for:

 + Cross-sell recommendations

 + Bundle discounts

 + "Customers also bought" placements

## Task 8: Calculate cohort map from product view to addtocart to purchase in Jan, Feb and March 2017. 

-Created three CTEs: one for product views, one for add-to-cart, one for purchases.

-Each filtered by eCommerceAction.action_type = 2 / 3 / 6.(hits.eCommerceAction.action_type = '2' is view product page; hits.eCommerceAction.action_type = '3' is add to cart; hits.eCommerceAction.action_type = '6' is purchase)

-Joined all three by month and calculated:
  
  + add_to_cart_rate = num_add_to_cart / num_product_view
  
  + purchase_rate = num_purchase / num_product_view

<img width="543" height="552" alt="image" src="https://github.com/user-attachments/assets/12673f07-07f8-46e7-9997-225bca0a079e" />

<img width="528" height="216" alt="image" src="https://github.com/user-attachments/assets/abaaf22b-4eab-4e69-8d73-d019995f3b06" />

<img width="898" height="104" alt="image" src="https://github.com/user-attachments/assets/f62ed06a-32cf-4dc0-a8f5-4f3d58d40a77" />

[Q8] Funnel Drop-offs (Jan–Mar 2017):

- Conversion rates improved steadily from January to March 2017 at both funnel stages.

- The add-to-cart rate rose from 28.5% → 37.3%, and purchase rate increased from 8.3% → 12.6%, showing better funnel efficiency over time.

- This could reflect improvements in product presentation, targeting, or checkout experience during Q1.



## 🔎 Final

## ✅ What I learned from this project:
- SQL Skills:

  + Gained confidence using complex queries with JOIN, CTE, UNNEST, CASE, and aggregation functions like COUNT, SUM, AVG, and ROUND.

  + Get more comfortable handling nested and repeated fields in BigQuery (e.g., hits, product, eCommerceAction).

- Cohort & Funnel Analysis Techniques:

  + Learned how to map user behavior across stages from product view → add to cart → purchase
 
  + Developed the ability to compute and compare conversion rates over time and by product
 
 - Marketing Data Storytelling

  + Practiced turning raw data into actionable business insights.

  + Learned how to write clear, logical conclusions based on quantitative analysis.

## 📊 Key Marketing Analytics Dimensions I Focused On:

-Traffic Quality

→ Evaluated traffic sources (Google, Direct, Social, etc.) using bounce rate, session count, and revenue contribution.

-User Behavior

→ Compared engagement metrics between purchasers and non-purchasers (pageviews, drop-off rate, interaction depth).

-Conversion Funnel

→ Measured step-by-step conversion rates from product view to cart to final purchase, identifying where users drop off.

-Product & Revenue Performance
→ Tracked revenue by traffic source and time (weekly/monthly)
