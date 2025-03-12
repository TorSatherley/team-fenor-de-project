\echo '===== DIM_COUNTERPARTY TABLE ====='
SELECT * FROM dim_counterparty
limit 10;

\echo '===== DIM_CURRENCY TABLE ====='
SELECT * FROM dim_currency
limit 10;

\echo '===== DIM_DATE TABLE ====='
SELECT * FROM dim_date
limit 10;

\echo '===== DIM_DESIGN TABLE ====='
SELECT * FROM dim_design
limit 10;

\echo '===== DIM_LOCATION TABLE ====='
SELECT * FROM dim_location
limit 10;

\echo '===== DIM_PAYMENT_TYPE TABLE ====='
SELECT * FROM dim_payment_type
limit 10;

\echo '===== DIM_STAFF TABLE ====='
SELECT * FROM dim_staff
limit 10;

\echo '===== DIM_TRANSACTION TABLE ====='
SELECT * FROM dim_transaction
limit 10;

\echo '===== FACT_PAYMENT TABLE ====='
SELECT * FROM fact_payment
limit 10;

\echo '===== FACT_PURCHASE_ORDER TABLE ====='
SELECT * FROM fact_purchase_order
limit 10;

\echo '===== FACT_SALES_ORDER TABLE ====='
SELECT * FROM fact_sales_order
limit 10;

-- PGPASSWORD='maSaxIhJnmv4bOk' psql -h nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com -U project_team_6 -d postgres -p 5432 -f print_warehouse.sql > warehouse.txt