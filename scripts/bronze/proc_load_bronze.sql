/*
===========================================================
Procedure Name : bronze.load_bronze
Layer          : Bronze (Staging Layer)
Author         : Nithiesh
Purpose        : Truncate and reload all bronze tables
Description    :
    This procedure refreshes the bronze layer by
    truncating existing data and loading fresh data
    from CRM and ERP CSV source files.
===========================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_start  TIMESTAMP;
    v_batch_end    TIMESTAMP;
    v_table_start  TIMESTAMP;
    v_table_end    TIMESTAMP;
    v_seconds      NUMERIC;
BEGIN
    v_batch_start := clock_timestamp();

    BEGIN
        RAISE NOTICE '=================================================';
        RAISE NOTICE '        STARTING BRONZE LAYER LOAD                ';
        RAISE NOTICE '=================================================';

        ----------------------------------------------------------------
        -- CRM TABLES
        ----------------------------------------------------------------
        RAISE NOTICE '-----------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '-----------------------------------------------';

        ----------------------------------------------------------------
        -- crm_cust_info
        ----------------------------------------------------------------
        v_table_start := clock_timestamp();

        RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
        COPY bronze.crm_cust_info
        FROM 'D:/sql/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        DELIMITER ',' CSV HEADER;

        v_table_end := clock_timestamp();
        v_seconds := EXTRACT(EPOCH FROM (v_table_end - v_table_start));
        RAISE NOTICE '>> Load Time (crm_cust_info): % seconds', v_seconds;


        ----------------------------------------------------------------
        -- crm_prd_info
        ----------------------------------------------------------------
        v_table_start := clock_timestamp();

        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
        COPY bronze.crm_prd_info
        FROM 'D:/sql/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        DELIMITER ',' CSV HEADER;

        v_table_end := clock_timestamp();
        v_seconds := EXTRACT(EPOCH FROM (v_table_end - v_table_start));
        RAISE NOTICE '>> Load Time (crm_prd_info): % seconds', v_seconds;


        ----------------------------------------------------------------
        -- crm_sales_details
        ----------------------------------------------------------------
        v_table_start := clock_timestamp();

        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
        COPY bronze.crm_sales_details
        FROM 'D:/sql/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        DELIMITER ',' CSV HEADER;

        v_table_end := clock_timestamp();
        v_seconds := EXTRACT(EPOCH FROM (v_table_end - v_table_start));
        RAISE NOTICE '>> Load Time (crm_sales_details): % seconds', v_seconds;


        ----------------------------------------------------------------
        -- ERP TABLES
        ----------------------------------------------------------------
        RAISE NOTICE '-----------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '-----------------------------------------------';

        ----------------------------------------------------------------
        -- erp_cust_az12
        ----------------------------------------------------------------
        v_table_start := clock_timestamp();

        RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12
        FROM 'D:/sql/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        DELIMITER ',' CSV HEADER;

        v_table_end := clock_timestamp();
        v_seconds := EXTRACT(EPOCH FROM (v_table_end - v_table_start));
        RAISE NOTICE '>> Load Time (erp_cust_az12): % seconds', v_seconds;


        ----------------------------------------------------------------
        -- erp_loc_a101
        ----------------------------------------------------------------
        v_table_start := clock_timestamp();

        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101
        FROM 'D:/sql/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        DELIMITER ',' CSV HEADER;

        v_table_end := clock_timestamp();
        v_seconds := EXTRACT(EPOCH FROM (v_table_end - v_table_start));
        RAISE NOTICE '>> Load Time (erp_loc_a101): % seconds', v_seconds;


        ----------------------------------------------------------------
        -- erp_px_cat_g1v2
        ----------------------------------------------------------------
        v_table_start := clock_timestamp();

        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2
        FROM 'D:/sql/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ',' CSV HEADER;

        v_table_end := clock_timestamp();
        v_seconds := EXTRACT(EPOCH FROM (v_table_end - v_table_start));
        RAISE NOTICE '>> Load Time (erp_px_cat_g1v2): % seconds', v_seconds;


        ----------------------------------------------------------------
        -- TOTAL BATCH TIME
        ----------------------------------------------------------------
        v_batch_end := clock_timestamp();
        v_seconds := EXTRACT(EPOCH FROM (v_batch_end - v_batch_start));

        RAISE NOTICE '=================================================';
        RAISE NOTICE 'BRONZE LAYER LOAD COMPLETED';
        RAISE NOTICE 'TOTAL BATCH LOAD TIME: % seconds', v_seconds;
        RAISE NOTICE '=================================================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '=============================================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error Message : %', SQLERRM;
            RAISE NOTICE 'Error Code    : %', SQLSTATE;
            RAISE NOTICE '=============================================';
            RAISE;
    END;
END;
$$;


--call bronze.load_bronze(); to run
