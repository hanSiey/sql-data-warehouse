/*
Insert data into silver tables from bronze data.
Insert data after applying the following transformations:
cleaning, transforming, and standardizing the data.
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		PRINT '===============';
		PRINT 'Loading silver layer data';
		PRINT '===============';

		--CRM_CUST_INFO TABLE--
		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM (
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flas_last
		FROM [bronze].crm_cust_info
		WHERE cst_id IS NOT NULL
		) AS t
		WHERE t.flas_last = 1

		--CRM_PROD_INFO TABLE--
		TRUNCATE TABLE silver.crm_prod_info;
		INSERT INTO silver.crm_prod_info (
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
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING( prd_key, 7, LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) as prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Standard'
			ELSE 'n/a'
		END prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prod_info

		--CRM_SALES_DETAILS TABLE--
		TRUNCATE TABLE silver.crm_sales_details;
		INSERT INTO silver.crm_sales_details(
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
		CASE
			WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
		END sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
		END sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
		END sls_due_dt,
		CASE
		   WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_quantity) 
		   THEN sls_quantity * ABS(sls_price)
		   ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price IS NULL OR sls_price <=0 
			THEN ABS(sls_sales) / NULLIF(sls_quantity,0)
			ELSE sls_price
		END sls_price
		FROM bronze.crm_sales_details

		--ERP_CUST_AZ12 TABLE--
		TRUNCATE TABLE silver.erp_cust_az12;
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		SELECT
		CASE 
		   WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
		   ELSE CID
		END AS CID,
		CASE
		 WHEN BDATE > GETDATE() THEN NULL
		 ELSE CAST(BDATE AS DATE)
		END BDATE,
		CASE
			WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
			WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
			ELSE 'n/a'
		END GEN
		FROM bronze.erp_cust_az12

		--ERP_LOC_A101 TABLE--
		TRUNCATE TABLE silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)
		SELECT
		REPLACE(CID,'-','') AS CID,
		CASE
			WHEN CNTRY = 'DE' THEN 'Germany'
			WHEN CNTRY IN ('US', 'USA') THEN 'United States'
			WHEN CNTRY IS NULL or CNTRY = '' THEN 'n/a'
			ELSE CNTRY
		END CNTRY
		FROM bronze.erp_loc_a101

		--ERP_PX_CAT_G1V2 TABLE--
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT
		*
		FROM bronze.erp_px_cat_g1v2

	END TRY
	BEGIN CATCH
		PRINT 'Error occurred while loading data: ' + ERROR_MESSAGE();
	END CATCH 
END
