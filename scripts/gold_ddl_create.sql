/*
CREATE VIEWS FOR GOLD LAYER

creates the views for the gold layer, which are based on the silver layer. The views are:
2 dimension tables and 1 fact table. The dimension tables are used to join the fact table and to filter the data. 
The fact table contains the sales information, which is the main focus of the analysis.
The dimension tables contain the customer and product information, which are used to analyze the sales data.
*/

CREATE VIEW gold.dim_customer AS
SELECT
ROW_NUMBER() OVER (ORDER BY c.cst_id) as id,
c.cst_id as customer_id,
c.cst_key as customer_key,
c.cst_firstname as firstname,
c.cst_lastname as lastname,
e.bdate as birthdate,
c.cst_marital_status as marital_status,
CASE
   WHEN c.cst_gndr != 'n/a' THEN c.cst_gndr
   ELSE COALESCE(e.gen, 'n/a')
END gender,
l.cntry as country,
c.cst_create_date as create_date,
c.dwh_create_date
FROM silver.crm_cust_info as c
LEFT JOIN silver.erp_cust_az12 as e
ON c.cst_key = e.cid
LEFT JOIN silver.erp_loc_a101 as l
ON c.cst_key = l.cid


CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER(ORDER BY p.prd_start_dt, p.prd_key) AS id,
p.prd_id as product_id,
p.prd_key as product_key,
p.prd_nm as product_name,
p.prd_line as product_line,
p.cat_id as category_id,
px.cat as category,
px.subcat as subcategory,
p.prd_cost as cost,
p.prd_start_dt as start_date,
px.maintenance
FROM silver.crm_prod_info AS p
LEFT JOIN silver.erp_px_cat_g1v2 AS px
ON p.cat_id = px.id
WHERE p.prd_end_dt IS NULL

CREATE VIEW gold.fact_sales AS
SELECT
s.sls_ord_num as order_number,
p.id as product_id,
c.id as customer_id,
s.sls_order_dt as order_date,
s.sls_ship_dt as shipping_date,
s.sls_due_dt as due_date,
s.sls_quantity as quantity,
s.sls_price as price,
s.sls_sales as sales
FROM silver.crm_sales_details AS s
LEFT JOIN gold.dim_products as p
ON s.sls_prd_key = p.product_key
LEFT JOIN gold.dim_customer as c
ON s.sls_cust_id = c.customer_id
