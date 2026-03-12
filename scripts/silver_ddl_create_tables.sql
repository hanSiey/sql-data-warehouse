/*
Create Tables
create tables in silver layer from bronze layer datasets. 
This is the first step in the data pipeline to move data from bronze to silver layer. 
The tables in silver layer will be used for further transformations and analysis.
*/

CREATE TABLE silver.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.crm_prod_info(
prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.erp_cust_az12(
CID NVARCHAR(50),
BDATE DATETIME,
GEN NVARCHAR(50),
dwh_create_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.erp_loc_a101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_create_date DATETIME DEFAULT GETDATE()
);

CREATE TABLE silver.erp_px_cat_g1v2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_create_date DATETIME DEFAULT GETDATE()
);
