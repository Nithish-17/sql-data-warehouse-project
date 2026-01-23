/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_start_time TIMESTAMP;
    v_batch_end_time   TIMESTAMP;
    v_start_time       TIMESTAMP;
    v_end_time         TIMESTAMP;
    v_error_message    TEXT;
BEGIN
    -------------------------------------------------
    -- Batch start
    -------------------------------------------------
    v_batch_start_time := clock_timestamp();

    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Starting Silver Layer Load';
    RAISE NOTICE 'Batch Start Time: %', v_batch_start_time;
    RAISE NOTICE '==============================================';

    -------------------------------------------------
    -- silver.crm_cust_info
    -------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '----------------------------------------------';
    RAISE NOTICE 'Truncating table: silver.crm_cust_info';

    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE 'Inserting into table: silver.crm_cust_info';

    INSERT INTO silver.crm_cust_info
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag = 1;

    v_end_time := clock_timestamp();
    RAISE NOTICE 'Finished silver.crm_cust_info. Duration: % seconds',
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));

    -------------------------------------------------
    -- silver.crm_prd_info
    -------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '----------------------------------------------';
    RAISE NOTICE 'Truncating table: silver.crm_prd_info';

    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE 'Inserting into table: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_'),
        SUBSTRING(prd_key FROM 7),
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        prd_start_dt::DATE,
        (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
            - INTERVAL '1 day')::DATE
    FROM bronze.crm_prd_info;

    v_end_time := clock_timestamp();
    RAISE NOTICE 'Finished silver.crm_prd_info. Duration: % seconds',
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));

    -------------------------------------------------
    -- silver.crm_sales_details
    -------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '----------------------------------------------';
    RAISE NOTICE 'Truncating table: silver.crm_sales_details';

    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE 'Inserting into table: silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        CASE 
            WHEN LENGTH(sls_order_dt::TEXT) <> 8 THEN NULL
            ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
        END,

        CASE 
            WHEN LENGTH(sls_ship_dt::TEXT) <> 8 THEN NULL
            ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
        END,

        CASE 
            WHEN LENGTH(sls_due_dt::TEXT) <> 8 THEN NULL
            ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
        END,

        CASE 
            WHEN sls_sales IS NULL 
              OR sls_sales <= 0 
              OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,

        sls_quantity,

        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 THEN
                CASE 
                    WHEN sls_quantity IS NULL OR sls_quantity = 0 THEN NULL
                    ELSE sls_sales / sls_quantity
                END
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    v_end_time := clock_timestamp();
    RAISE NOTICE 'Finished silver.crm_sales_details. Duration: % seconds',
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));

    -------------------------------------------------
    -- silver.erp_cust_az12
    -------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '----------------------------------------------';
    RAISE NOTICE 'Truncating table: silver.erp_cust_az12';

    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE 'Inserting into table: silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
            ELSE cid
        END,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    v_end_time := clock_timestamp();
    RAISE NOTICE 'Finished silver.erp_cust_az12. Duration: % seconds',
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));

    -------------------------------------------------
    -- silver.erp_loc_a101
    -------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '----------------------------------------------';
    RAISE NOTICE 'Truncating table: silver.erp_loc_a101';

    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE 'Inserting into table: silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101
    SELECT 
        REPLACE(cid, '-', ''),
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE cntry
        END
    FROM bronze.erp_loc_a101;

    v_end_time := clock_timestamp();
    RAISE NOTICE 'Finished silver.erp_loc_a101. Duration: % seconds',
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));

    -------------------------------------------------
    -- silver.erp_px_cat_g1v2
    -------------------------------------------------
    v_start_time := clock_timestamp();
    RAISE NOTICE '----------------------------------------------';
    RAISE NOTICE 'Truncating table: silver.erp_px_cat_g1v2';

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE 'Inserting into table: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2
    SELECT 
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    v_end_time := clock_timestamp();
    RAISE NOTICE 'Finished silver.erp_px_cat_g1v2. Duration: % seconds',
        EXTRACT(EPOCH FROM (v_end_time - v_start_time));

    -------------------------------------------------
    -- Batch end
    -------------------------------------------------
    v_batch_end_time := clock_timestamp();

    RAISE NOTICE '==============================================';
    RAISE NOTICE 'Silver Layer Load Completed Successfully';
    RAISE NOTICE 'Batch End Time: %', v_batch_end_time;
    RAISE NOTICE 'Total Batch Duration: % seconds',
        EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time));
    RAISE NOTICE '==============================================';


EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        v_batch_end_time := clock_timestamp();

        RAISE NOTICE '==============================================';
        RAISE NOTICE '❌ ERROR DURING SILVER LOAD';
        RAISE NOTICE 'Error Message: %', v_error_message;
        RAISE NOTICE 'Batch Start Time: %', v_batch_start_time;
        RAISE NOTICE 'Failure Time: %', v_batch_end_time;
        RAISE NOTICE 'Elapsed Time: % seconds',
            EXTRACT(EPOCH FROM (v_batch_end_time - v_batch_start_time));
        RAISE NOTICE '==============================================';

        RAISE; -- rethrow error
END;
$$;
