/*
==Full Load data==
This script performs bulk insert operations to load data from CSV files into the corresponding tables in the 'bronze' schema of the 'DataWarehouse' database. 
Each table is truncated before the bulk insert to ensure that only the latest data is present. 
The bulk insert operations specify the file path, field terminator, row terminator, and other relevant options to correctly parse the CSV files.
*/

EXEC bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @batch_start DATETIME,@batch_end DATETIME;
	BEGIN TRY
	    PRINT '===============';
		PRINT 'Loading data';
		PRINT '===============';
		SET @batch_start = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM --absolute path to the csv file--
		WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 ROWTERMINATOR = '\n',
		 TABLOCK
		);

		TRUNCATE TABLE bronze.crm_prod_info;
		BULK INSERT bronze.crm_prod_info
		FROM --absolute path to the csv file--
		WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 ROWTERMINATOR = '\n',
		 TABLOCK
		);

		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM --absolute path to the csv file--
		WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 ROWTERMINATOR = '\n',
		 TABLOCK
		);

		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM --absolute path to the csv file--
		WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 ROWTERMINATOR = '\n',
		 TABLOCK
		);

		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM --absolute path to the csv file--
		WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 ROWTERMINATOR = '\n',
		 TABLOCK
		);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM --absolute path to the csv file--
		WITH (
		 FIRSTROW = 2,
		 FIELDTERMINATOR = ',',
		 ROWTERMINATOR = '\n',
		 TABLOCK
		);
		SET @batch_end = GETDATE();

		PRINT '===============';
		PRINT 'Data loaded successfully in ' + CAST(DATEDIFF(SECOND, @batch_start, @batch_end) AS VARCHAR) + ' seconds.';
		PRINT '===============';
	END TRY
	BEGIN CATCH
		PRINT 'Error occurred while loading data: ' + ERROR_MESSAGE();
	END CATCH 
END
