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

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();

        BEGIN TRANSACTION;

        PRINT '======================================';
        PRINT 'LOADING SILVER LAYER';
        PRINT '======================================';


        -- ======================================
        -- LOADING CRM TABLES
        -- ======================================

        PRINT '--------------------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '--------------------------------------';


        -- 1. CRM CUSTOMER INFO

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: silver.crm_cust_info';

        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting data into: silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,

            CASE
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_material_status,

            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,

            cst_create_date

        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id
                    ORDER BY cst_create_date DESC
                ) AS flag_last

            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL

        ) AS t

        WHERE flag_last = 1;

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '--------------------------------------';


        -- 2. CRM PRODUCT INFO

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: silver.crm_prd_info';

        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting data into: silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            prd_id,

            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,

            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,

            prd_nm,

            ISNULL(prd_cost, 0) AS prd_cost,

            CASE
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                ELSE 'n/a'
            END AS prd_line,

            CAST(prd_start_dt AS DATE) AS prd_start_dt,

            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1 AS DATE
            ) AS prd_end_dt

        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '--------------------------------------';


        -- 3. CRM SALES DETAILS

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: silver.crm_sales_details';

        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting data into: silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,

            TRY_CONVERT(
                DATE,
                CAST(NULLIF(sls_order_dt, 0) AS VARCHAR(8)),
                112
            ) AS sls_order_dt,

            TRY_CONVERT(
                DATE,
                CAST(NULLIF(sls_ship_dt, 0) AS VARCHAR(8)),
                112
            ) AS sls_ship_dt,

            TRY_CONVERT(
                DATE,
                CAST(NULLIF(sls_due_dt, 0) AS VARCHAR(8)),
                112
            ) AS sls_due_dt,

            CASE
                WHEN sls_sales IS NULL
                  OR sls_sales <= 0
                  OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,

            sls_quantity,

            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price

        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '--------------------------------------';


        -- ======================================
        -- LOADING ERP TABLES
        -- ======================================

        PRINT '======================================';
        PRINT 'LOADING ERP TABLES';
        PRINT '======================================';


        -- 4. ERP CUSTOMER AZ12

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: silver.erp_CUST_AZ12';

        TRUNCATE TABLE silver.erp_CUST_AZ12;

        PRINT '>> Inserting data into: silver.erp_CUST_AZ12';

        INSERT INTO silver.erp_CUST_AZ12 (
            CID,
            BDATE,
            GEN
        )
        SELECT
            CASE
                WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
                ELSE CID
            END AS CID,

            CASE
                WHEN BDATE > GETDATE() THEN NULL
                ELSE BDATE
            END AS BDATE,

            CASE
                WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS GEN

        FROM bronze.erp_CUST_AZ12;

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '--------------------------------------';


        -- 5. ERP LOCATION A101

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: silver.erp_LOC_A101';

        TRUNCATE TABLE silver.erp_LOC_A101;

        PRINT '>> Inserting data into: silver.erp_LOC_A101';

        INSERT INTO silver.erp_LOC_A101 (
            CID,
            CNTRY
        )
        SELECT
            REPLACE(CID, '-', '') AS CID,

            CASE
                WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
                WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
                ELSE TRIM(CNTRY)
            END AS CNTRY

        FROM bronze.erp_LOC_A101;

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '--------------------------------------';


        -- 6. ERP PRODUCT CATEGORY G1V2

        SET @start_time = GETDATE();

        PRINT '>> Truncating table: silver.erp_PX_CAT_G1V2';

        TRUNCATE TABLE silver.erp_PX_CAT_G1V2;

        PRINT '>> Inserting data into: silver.erp_PX_CAT_G1V2';

        INSERT INTO silver.erp_PX_CAT_G1V2 (
            ID,
            CAT,
            SUBCAT,
            MAINTENANCE
        )
        SELECT
            ID,
            CAT,
            SUBCAT,
            MAINTENANCE

        FROM bronze.erp_PX_CAT_G1V2;

        SET @end_time = GETDATE();

        PRINT '>> Load Duration: '
            + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
            + ' seconds';

        PRINT '--------------------------------------';


        -- ======================================
        -- COMPLETION
        -- ======================================

        COMMIT TRANSACTION;

        SET @batch_end_time = GETDATE();

        PRINT '======================================';
        PRINT 'SILVER LAYER LOADED SUCCESSFULLY';
        PRINT 'TOTAL BATCH DURATION: '
            + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)
            + ' seconds';
        PRINT '======================================';

    END TRY

    BEGIN CATCH

        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '======================================';
        PRINT 'ERROR OCCURRED DURING SILVER LOADING';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT 'ERROR LINE: ' + CAST(ERROR_LINE() AS NVARCHAR);
        PRINT '======================================';

        THROW;

    END CATCH

END;
GO

