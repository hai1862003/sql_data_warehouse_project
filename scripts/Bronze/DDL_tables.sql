/*
---------------------
Create Tables DDL (structure)
----------------------
Purpose:
	This scripts creates all tables structure from 2 source system ('crm', 'erp').
	Naming convention is followed for table naming (sourcesystem_table)
*/
-- Create Table structure for database 'DataWareHouse'
USE DataWareHouse;
GO

CREATE TABLE bronze.crm_cust_info ( 
	cst_id INT,
	cst_key VARCHAR(20),
	cst_firstname VARCHAR(20),
	cst_lastname VARCHAR(20),
	cst_marital_status VARCHAR(5),
	cst_gndr VARCHAR(5), 
	cst_create_date DATE  -- must check compatible date format
); 

CREATE TABLE bronze.crm_prod_info (
	prd_id INT,	
	prd_key VARCHAR(30),	
	prd_nm VARCHAR(50),	
	prd_cost INT,	
	prd_line VARCHAR(5),	
	prd_start_dt DATE,	
	prd_end_dt DATE
 
);

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num VARCHAR(20),	
	sls_prd_key VARCHAR(20),	
	sls_cust_id INT,	
	sls_order_dt INT,	
	sls_ship_dt INT,	
	sls_due_dt INT,	
	sls_sales INT,	
	sls_quantity INT,	
	sls_price INT
);

CREATE TABLE bronze.erp_cust_az12 (
	CID VARCHAR(50),	
	BDATE DATE,	
	GEN VARCHAR(10)
);

CREATE TABLE bronze.erp_loc_a101 (
	CID VARCHAR(20),
	CNTRY VARCHAR(20)
);

CREATE TABLE bronze.erp_px_cat_g1v2(
	ID VARCHAR(10),	
	CAT VARCHAR(30),	
	SUBCAT VARCHAR(30),	
	MAINTENANCE VARCHAR(10)
);

