/*
===============================================================================
DDL Script: Create Bronze Tables (PostgreSQL)
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' tables.
===============================================================================
*/

-- Ensure bronze schema exists
CREATE SCHEMA IF NOT EXISTS bronze;

------------------------------------------------------------
-- bronze.crm_cust_info
------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             TEXT,
    cst_firstname       TEXT,
    cst_lastname        TEXT,
    cst_marital_status  TEXT,
    cst_gndr            TEXT,
    cst_create_date     DATE
);

------------------------------------------------------------
-- bronze.crm_prd_info
------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id        INT,
    prd_key       TEXT,
    prd_nm        TEXT,
    prd_cost      INT,
    prd_line      TEXT,
    prd_start_dt  TIMESTAMP,
    prd_end_dt    TIMESTAMP
);

------------------------------------------------------------
-- bronze.crm_sales_details
------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   TEXT,
    sls_prd_key   TEXT,
    sls_cust_id   INT,
    sls_order_dt  INT,
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
);

------------------------------------------------------------
-- bronze.erp_loc_a101
------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
    cid    TEXT,
    cntry  TEXT
);

------------------------------------------------------------
-- bronze.erp_cust_az12
------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid    TEXT,
    bdate  DATE,
    gen    TEXT
);

------------------------------------------------------------
-- bronze.erp_px_cat_g1v2
------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           TEXT,
    cat          TEXT,
    subcat       TEXT,
    maintenance  TEXT
);
