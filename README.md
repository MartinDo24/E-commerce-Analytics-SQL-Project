# E-commerce-Analytics-SQL-Project
Thực hiện phân tích dữ liệu Google Analytics mẫu để hiểu hành vi khách hàng, hành trình mua sắm và hiệu quả chuyển đổi.
🔍 Mục tiêu phân tích
- Lượt truy cập và lượt xem trang theo tháng
- Tỷ lệ thoát (bounce rate) theo nguồn truy cập
- Doanh thu theo nguồn và theo thời gian (tuần/tháng)
- So sánh hành vi người mua vs. không mua
- Phân tích sản phẩm hay được mua kèm
- Tính tỷ lệ thêm vào giỏ hàng và tỷ lệ mua hàng
🧰 Công cụ sử dụng
- SQL (trên Google BigQuery)
- Dữ liệu: `bigquery-public-data.google_analytics_sample`
  📂 File chính
- `ecommerce_analytics.sql`: chứa toàn bộ các truy vấn phân tích (Q1 → Q8)
  
🔗 Dataset công khai: https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1sbigquery-public-data!2sgoogle_analytics_sample!3sga_sessions_20170801
