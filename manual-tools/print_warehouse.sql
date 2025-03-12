\echo '===== DIM_COUNTERPARTY TABLE ====='
SELECT * FROM dim_counterparty;

\echo '===== DIM_CURRENCY TABLE ====='
SELECT * FROM dim_currency;

\echo '===== DIM_DATE TABLE ====='
SELECT * FROM dim_date;

\echo '===== DIM_DESIGN TABLE ====='
SELECT * FROM dim_design;

\echo '===== DIM_LOCATION TABLE ====='
SELECT * FROM dim_location;

\echo '===== DIM_PAYMENT_TYPE TABLE ====='
SELECT * FROM dim_payment_type;

\echo '===== DIM_STAFF TABLE ====='
SELECT * FROM dim_staff;

\echo '===== DIM_TRANSACTION TABLE ====='
SELECT * FROM dim_transaction;

\echo '===== FACT_PAYMENT TABLE ====='
SELECT * FROM fact_payment;

\echo '===== FACT_PURCHASE_ORDER TABLE ====='
SELECT * FROM fact_purchase_order;

\echo '===== FACT_SALES_ORDER TABLE ====='
SELECT * FROM fact_sales_order;

-- PGPASSWORD='maSaxIhJnmv4bOk' psql -h nc-data-eng-project-dw-prod.chpsczt8h1nu.eu-west-2.rds.amazonaws.com -U project_team_6 -d postgres -p 5432 -f print_warehouse.sql > warehouse.txt