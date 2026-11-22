-- =============================================================================
-- E-Commerce Customer Behaviour & Sales Intelligence
-- Databricks SQL / AI/BI (Lakeview) Dashboard Queries
-- =============================================================================
-- Catalog / Schema: workspace.ecommerce
-- Target Layer    : Gold Delta tables (with Silver slice query for global filters)
--
-- Recommended Dashboard Global Filters:
--   1. Date Range : order_date / year_month
--   2. State      : state (2-letter Brazilian UF code, e.g., SP, RJ, MG)
--   3. Category   : category_en (English product category)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- VISUAL 1: Executive KPI Counters
-- Visual Type: 6 KPI Counter Tiles (Revenue, Orders, AOV, Unique Customers,
--              % Late Deliveries, Avg Review Score)
-- Filters    : Supports Date Range, State, and Category global filters.
-- -----------------------------------------------------------------------------
WITH order_level AS (
    SELECT
        order_id,
        MAX(customer_unique_id)      AS customer_unique_id,
        MAX(CAST(is_late AS INT))    AS is_late_flag,
        AVG(review_score)            AS order_review_score,
        SUM(item_total)              AS order_revenue
    FROM workspace.ecommerce.silver_orders_enriched
    WHERE 1 = 1
      -- Uncomment in Databricks AI/BI Dashboard when binding global parameters:
      -- AND order_date BETWEEN :start_date AND :end_date
      -- AND (:state = 'ALL' OR customer_state = :state)
      -- AND (:category = 'ALL' OR category_en = :category)
    GROUP BY order_id
)
SELECT
    ROUND(SUM(order_revenue), 2)                             AS total_revenue_brl,
    COUNT(DISTINCT order_id)                                 AS total_orders,
    ROUND(SUM(order_revenue) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS aov_brl,
    COUNT(DISTINCT customer_unique_id)                       AS unique_customers,
    ROUND(AVG(CAST(is_late_flag AS DOUBLE)) * 100.0, 2)      AS pct_late_deliveries,
    ROUND(AVG(order_review_score), 2)                        AS avg_review_score
FROM order_level;


-- -----------------------------------------------------------------------------
-- VISUAL 2: Monthly Revenue Trend & MoM Growth %
-- Visual Type: Dual-Axis Combo Chart (Bar: revenue, Line: mom_growth_pct)
-- Source     : workspace.ecommerce.gold_monthly_sales
-- -----------------------------------------------------------------------------
SELECT
    year_month,
    revenue,
    orders,
    ROUND(revenue / NULLIF(orders, 0), 2) AS monthly_aov,
    mom_growth_pct
FROM workspace.ecommerce.gold_monthly_sales
ORDER BY year_month ASC;


-- -----------------------------------------------------------------------------
-- VISUAL 3: Top 10 Product Categories by Revenue
-- Visual Type: Horizontal Bar Chart + KPI Tooltip (units, avg_price, avg_review)
-- Source     : workspace.ecommerce.gold_category_performance
-- -----------------------------------------------------------------------------
SELECT
    category_en,
    units,
    revenue,
    avg_price,
    avg_review,
    ROUND(
        revenue * 100.0 / SUM(revenue) OVER (),
        2
    ) AS revenue_share_pct
FROM workspace.ecommerce.gold_category_performance
WHERE category_en <> 'unknown'
ORDER BY revenue DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- VISUAL 4: Revenue & Delivery SLA by Brazilian State
-- Visual Type: Choropleth Map (Brazil UF codes) + Ranked State Table
-- Source     : workspace.ecommerce.gold_state_sales + gold_delivery_performance
-- -----------------------------------------------------------------------------
SELECT
    s.state,
    s.orders,
    s.revenue,
    s.customers,
    ROUND(s.revenue / NULLIF(s.orders, 0), 2) AS state_aov,
    d.avg_delivery_days,
    d.avg_delay_days,
    d.pct_late,
    d.avg_review
FROM workspace.ecommerce.gold_state_sales s
LEFT JOIN workspace.ecommerce.gold_delivery_performance d
    ON s.state = d.state
ORDER BY s.revenue DESC;


-- -----------------------------------------------------------------------------
-- VISUAL 5: Payment Mix Breakdown
-- Visual Type: Donut / Bar Chart (Revenue & Order Share by Payment Method)
-- Source     : workspace.ecommerce.gold_payment_mix
-- -----------------------------------------------------------------------------
SELECT
    payment_type,
    orders,
    revenue,
    ROUND(revenue * 100.0 / SUM(revenue) OVER (), 2) AS revenue_share_pct,
    ROUND(orders * 100.0 / SUM(orders) OVER (), 2)   AS order_share_pct,
    avg_installments
FROM workspace.ecommerce.gold_payment_mix
ORDER BY revenue DESC;


-- -----------------------------------------------------------------------------
-- VISUAL 6: Customer RFM Segment Distribution
-- Visual Type: Stacked Bar / Treemap (Customers & Revenue by RFM Segment)
-- Source     : workspace.ecommerce.gold_customer_rfm
-- -----------------------------------------------------------------------------
SELECT
    rfm_segment,
    COUNT(DISTINCT customer_unique_id)                        AS customers,
    ROUND(
        COUNT(DISTINCT customer_unique_id) * 100.0
        / SUM(COUNT(DISTINCT customer_unique_id)) OVER (),
        2
    )                                                         AS customer_share_pct,
    ROUND(SUM(monetary), 2)                                   AS total_revenue,
    ROUND(
        SUM(monetary) * 100.0 / SUM(SUM(monetary)) OVER (),
        2
    )                                                         AS revenue_share_pct,
    ROUND(AVG(recency_days), 1)                               AS avg_recency_days,
    ROUND(AVG(frequency), 2)                                  AS avg_frequency,
    ROUND(AVG(monetary), 2)                                   AS avg_monetary_brl
FROM workspace.ecommerce.gold_customer_rfm
GROUP BY rfm_segment
ORDER BY
    CASE rfm_segment
        WHEN 'Champions'          THEN 1
        WHEN 'Loyal'              THEN 2
        WHEN 'Potential Loyalist' THEN 3
        WHEN 'At Risk'            THEN 4
        WHEN 'Lost'               THEN 5
        ELSE 6
    END;


-- -----------------------------------------------------------------------------
-- VISUAL 7: Delivery Delay Bucket vs. Customer Review Score
-- Visual Type: Combo Chart (Bar: orders, Line: avg_review)
-- Source     : workspace.ecommerce.gold_review_vs_delay
-- -----------------------------------------------------------------------------
SELECT
    delay_bucket,
    avg_review,
    orders,
    ROUND(orders * 100.0 / SUM(orders) OVER (), 2) AS order_share_pct
FROM workspace.ecommerce.gold_review_vs_delay
ORDER BY bucket_order ASC;


-- -----------------------------------------------------------------------------
-- VISUAL 8: Top 10 Sellers by Revenue
-- Visual Type: Horizontal Bar Chart / Leaderboard Table
-- Source     : workspace.ecommerce.gold_seller_performance
-- -----------------------------------------------------------------------------
SELECT
    seller_id,
    revenue,
    orders,
    ROUND(revenue / NULLIF(orders, 0), 2) AS avg_order_revenue,
    avg_review
FROM workspace.ecommerce.gold_seller_performance
ORDER BY revenue DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- BONUS VISUAL 9: Daily Sales & Freight Trend (Filterable Time Series)
-- Visual Type: Area / Line Chart (Daily Revenue & 7-Day Moving Average)
-- Source     : workspace.ecommerce.gold_daily_sales
-- -----------------------------------------------------------------------------
SELECT
    order_date,
    orders,
    revenue,
    aov,
    freight,
    ROUND(
        AVG(revenue) OVER (
            ORDER BY order_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
        2
    ) AS revenue_7d_ma
FROM workspace.ecommerce.gold_daily_sales
ORDER BY order_date ASC;
