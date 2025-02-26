DROP DATABASE IF EXISTS fenor_data_warehouse;
CREATE DATABASE fenor_data_warehouse;

\echo '🎉 Initialised test database! 🎉'

CREATE TABLE fact_sales_order (
  sales_record_id SERIAL PRIMARY KEY,
  sales_order_id int NOT NULL,
  created_date date NOT NULL,
  created_time time NOT NULL,
  last_updated_date date NOT NULL,
  last_updated_time time NOT NULL,
  sales_staff_id int NOT NULL,
  counterparty_id int NOT NULL,
  units_sold int NOT NULL,
  unit_price numeric(10, 2) NOT NULL,
  currency_id int NOT NULL,
  design_id int NOT NULL,
  agreed_payment_date date NOT NULL,
  agreed_delivery_date date NOT NULL,
  agreed_delivery_location_id int NOT NULL
);

CREATE TABLE fact_purchase_orders (
  purchase_record_id SERIAL PRIMARY KEY,
  purchase_order_id INT NOT NULL,
  created_date date NOT NULL,
  created_time time NOT NULL,
  last_updated_date date NOT NULL,
  last_updated_time time NOT NULL,
  staff_id int NOT NULL,
  counterparty_id int NOT NULL,
  item_code varchar NOT NULL,
  item_quantity int NOT NULL,
  item_unit_price numeric NOT NULL,
  currency_id int NOT NULL,
  agreed_delivery_date date NOT NULL,
  agreed_payment_date date NOT NULL,
  agreed_delivery_location_id int NOT NULL
);

CREATE TABLE fact_payment (

);

CREATE TABLE dim_transaction (

);

CREATE TABLE dim_staff (

);

CREATE TABLE dim_payment_type (

);

CREATE TABLE dim_location (

);

CREATE TABLE dim_design (

);

CREATE TABLE dim_date (

);

CREATE TABLE dim_currency (

);

CREATE TABLE dim_counterparty (

);

