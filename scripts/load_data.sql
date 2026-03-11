/*
==Full Load data==
This script performs bulk insert operations to load data from CSV files into the corresponding tables in the 'bronze' schema of the 'DataWarehouse' database. 
Each table is truncated before the bulk insert to ensure that only the latest data is present. 
The bulk insert operations specify the file path, field terminator, row terminator, and other relevant options to correctly parse the CSV files.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
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
	FROM 'C:\Users\hants\Documents\Certifications & Qualifications\Data Engineering\SQL\Projects\datasets\erp\PX_CAT_G1V2.csv'
	WITH (
	 FIRSTROW = 2,
	 FIELDTERMINATOR = ',',
	 ROWTERMINATOR = '\n',
	 TABLOCK
	);
END
