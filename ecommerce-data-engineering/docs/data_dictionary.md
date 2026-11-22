# Data Dictionary — E-Commerce Customer Behaviour & Sales Intelligence

All tables live in Unity Catalog under `workspace.ecommerce` (configurable via [`config/config.py`](../config/config.py)).

---

## 1. Bronze Layer (9 Delta Tables)

Every Bronze table preserves the raw CSV schema plus two audit metadata columns:
- `_ingested_at` (`TIMESTAMP`): UTC timestamp when the batch was ingested.
- `_source_file` (`STRING`): Full Unity Catalog Volume path of the source CSV (`_metadata.file_path`).

| Table | Source CSV | Key / Grain | Key Columns |
| :--- | :--- | :--- | :--- |
| `bronze_orders` | `olist_orders_dataset.csv` | `order_id` | `order_id`, `customer_id`, `order_status`, 5 raw timestamp strings |
| `bronze_order_items` | `olist_order_items_dataset.csv` | `(order_id, order_item_id)` | `order_id`, `order_item_id`, `product_id`, `seller_id`, `shipping_limit_date`, `price`, `freight_value` |
| `bronze_order_payments` | `olist_order_payments_dataset.csv` | `(order_id, payment_sequential)` | `order_id`, `payment_sequential`, `payment_type`, `payment_installments`, `payment_value` |
| `bronze_order_reviews` | `olist_order_reviews_dataset.csv` | `review_id` (raw has dups) | `review_id`, `order_id`, `review_score`, `review_comment_title`, `review_comment_message`, `review_creation_date`, `review_answer_timestamp` |
| `bronze_customers` | `olist_customers_dataset.csv` | `customer_id` | `customer_id`, `customer_unique_id`, `customer_zip_code_prefix`, `customer_city`, `customer_state` |
| `bronze_products` | `olist_products_dataset.csv` | `product_id` | `product_id`, `product_category_name`, `product_name_lenght`, `product_description_lenght`, `product_photos_qty`, physical dimensions |
| `bronze_sellers` | `olist_sellers_dataset.csv` | `seller_id` | `seller_id`, `seller_zip_code_prefix`, `seller_city`, `seller_state` |
| `bronze_geolocation` | `olist_geolocation_dataset.csv` | Many rows per zip prefix | `geolocation_zip_code_prefix`, `geolocation_lat`, `geolocation_lng`, `geolocation_city`, `geolocation_state` |
| `bronze_category_translation` | `product_category_name_translation.csv` | `product_category_name` | `product_category_name`, `product_category_name_english` |

---

## 2. Silver Layer (10 Delta Tables)

### `silver_orders` (Delivered Orders) & `silver_orders_non_delivered`
- **Grain**: One unique row per `order_id`.
- **Partition Split**: `silver_orders` holds `order_status = 'delivered'`; `silver_orders_non_delivered` holds all other statuses (`canceled`, `unavailable`, `shipped`, `invoiced`, `processing`, `created`, `approved`).

| Column | Type | Description |
| :--- | :--- | :--- |
| `order_id` | `STRING` (PK) | Unique order identifier |
| `customer_id` | `STRING` (FK) | Per-order customer key linking to `silver_customers` |
| `order_status` | `STRING` | Standardized lowercase order status |
| `order_purchase_timestamp` | `TIMESTAMP` | Timestamp when customer placed the order |
| `order_approved_at` | `TIMESTAMP` | Payment approval timestamp |
| `order_delivered_carrier_date` | `TIMESTAMP` | Handoff timestamp to logistics carrier |
| `order_delivered_customer_date` | `TIMESTAMP` | Actual delivery timestamp to customer (nullified if `< purchase_timestamp`) |
| `order_estimated_delivery_date` | `TIMESTAMP` | Promised delivery estimate shown at checkout |
| `order_date` | `DATE` | Calendar date of purchase |
| `order_year` | `INT` | Year extracted from `order_purchase_timestamp` |
| `order_month` | `INT` | Month (1–12) extracted from `order_purchase_timestamp` |
| `year_month` | `STRING` | Formatted `yyyy-MM` string for time-series grouping |
| `delivery_days` | `INT` | `datediff(order_delivered_customer_date, order_purchase_timestamp)` |
| `delay_days` | `INT` | `datediff(order_delivered_customer_date, order_estimated_delivery_date)` (`> 0` = late) |
| `is_late` | `BOOLEAN` | `TRUE` if `delay_days > 0`, `FALSE` if `<= 0`, `NULL` if delivery date missing |

### `silver_order_items`
- **Grain**: One unique row per `(order_id, order_item_id)`.

| Column | Type | Description |
| :--- | :--- | :--- |
| `order_id` | `STRING` (PK) | Order identifier |
| `order_item_id` | `INT` (PK) | Sequential item number within an order |
| `product_id` | `STRING` (FK) | Product identifier |
| `seller_id` | `STRING` (FK) | Marketplace seller identifier |
| `shipping_limit_date` | `TIMESTAMP` | Seller SLA deadline to hand item to carrier |
| `price` | `DOUBLE` | Unit item price in BRL (`>= 0`) |
| `freight_value` | `DOUBLE` | Allocated freight/shipping cost in BRL (`>= 0`) |
| `item_total` | `DOUBLE` | Derived: `round(price + freight_value, 2)` |

### `silver_order_payments`
- **Grain**: One unique row per `order_id` (pre-aggregated to prevent join fan-out).

| Column | Type | Description |
| :--- | :--- | :--- |
| `order_id` | `STRING` (PK) | Order identifier |
| `total_payment_value` | `DOUBLE` | Sum of all `payment_value` rows for the order |
| `max_installments` | `INT` | Maximum installment count chosen for the order |
| `payment_count` | `INT` | Number of payment records associated with the order |
| `primary_payment_type` | `STRING` | Payment method contributing the highest `payment_value` for the order |

### `silver_order_reviews`
- **Grain**: One unique row per `review_id` (deduplicated keeping latest answer/creation timestamp).
- **Columns**: `review_id`, `order_id`, `review_score` (`INT`, 1–5), `review_comment_title`, `review_comment_message`, `review_creation_date`, `review_answer_timestamp`.

### `silver_customers`
- **Grain**: One unique row per `customer_id`.
- **Columns**: `customer_id`, `customer_unique_id` (true cross-order person ID), `customer_zip_code_prefix` (5-char zero-padded), `customer_city` (lowercase), `customer_state` (2-letter uppercase UF).

### `silver_products`
- **Grain**: One unique row per `product_id`.
- **Columns**: `product_id`, `product_category_name` (nulls imputed with `"unknown"`), `category_en` (English translation with fallback), `product_name_length`, `product_description_length`, `product_photos_qty`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`.

### `silver_sellers`
- **Grain**: One unique row per `seller_id`.
- **Columns**: `seller_id`, `seller_zip_code_prefix` (5-char zero-padded), `seller_city` (lowercase), `seller_state` (uppercase UF).

### `silver_geolocation`
- **Grain**: One unique row per 5-digit `geolocation_zip_code_prefix`.
- **Columns**: `geolocation_zip_code_prefix`, `geolocation_lat` (average), `geolocation_lng` (average), `geolocation_city`, `geolocation_state`, `coordinate_samples` (count of raw coordinates averaged).

### `silver_orders_enriched`
- **Grain**: One unique row per `(order_id, order_item_id)` across all delivered orders.
- **Purpose**: Single conformed analytical fact table joining delivered `silver_orders`, `silver_order_items`, `silver_order_payments`, `silver_customers`, `silver_products`, `silver_sellers`, and order-level aggregated `silver_order_reviews`.

---

## 3. Gold Layer (9 Delta Tables)

| Table | Grain | Columns | Business Definition |
| :--- | :--- | :--- | :--- |
| `gold_daily_sales` | `order_date` | `order_date`, `orders`, `revenue`, `aov`, `freight` | Daily GMV (`SUM(item_total)`), distinct orders, average order value (`revenue / orders`), and total freight |
| `gold_monthly_sales` | `year_month` | `year_month`, `revenue`, `orders`, `mom_growth_pct` | Monthly revenue, orders, and Month-over-Month % revenue growth computed via `lag(revenue, 1)` |
| `gold_category_performance` | `category_en` | `category_en`, `units`, `revenue`, `avg_price`, `avg_review` | English product category unit volume, total revenue, average item price, and average customer review score |
| `gold_customer_rfm` | `customer_unique_id` | `customer_unique_id`, `recency_days`, `frequency`, `monetary`, `r_score`, `f_score`, `m_score`, `rfm_score`, `rfm_segment` | Customer-level RFM metrics scored 1–5 via `ntile(5)` and segmented into `Champions`, `Loyal`, `Potential Loyalist`, `At Risk`, `Lost` |
| `gold_state_sales` | `state` | `state`, `orders`, `revenue`, `customers` | Regional sales footprint by Brazilian state (`customer_state`), including unique customers |
| `gold_delivery_performance` | `state` | `state`, `avg_delivery_days`, `avg_delay_days`, `pct_late`, `avg_review` | Order-grain logistics SLA performance and customer satisfaction by state |
| `gold_payment_mix` | `payment_type` | `payment_type`, `orders`, `revenue`, `avg_installments` | Revenue, order volume, and order-grain average installments per primary payment method |
| `gold_seller_performance` | `seller_id` | `seller_id`, `revenue`, `orders`, `avg_review` | Seller leaderboard tracking total revenue, distinct orders fulfilled, and average customer review |
| `gold_review_vs_delay` | `delay_bucket` | `delay_bucket`, `bucket_order`, `avg_review`, `orders` | Order-grain customer review score across delay tiers: `On Time`, `1-3 Days Late`, `4-7 Days Late`, `8+ Days Late` |
