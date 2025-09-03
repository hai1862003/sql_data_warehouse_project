/*
---------------------
Create Tables DDL-Silver (structure)
----------------------
Purpose:
	This scripts creates all tables structure for Silver layer.
    Structure = Bronze Layer + Metadata columns
	Naming convention is followed for table/column naming 
*/


-- Create Table structure for database 'DataWareHouse'
USE DataWareHouse;
GO

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL 
	DROP TABLE silver.crm_cust_info
CREATE TABLE silver.crm_cust_info ( 
	cst_id INT,
	cst_key VARCHAR(20),
	cst_firstname VARCHAR(20),
	cst_lastname VARCHAR(20),
	cst_marital_status VARCHAR(20),
	cst_gndr VARCHAR(20), 
	cst_create_date DATE,  -- must check compatible date format
	dwh_create_date DATETIME2 DEFAULT GETDATE()
); 

IF OBJECT_ID('silver.crm_prod_info', 'U') IS NOT NULL 
	DROP TABLE silver.crm_prod_info
CREATE TABLE silver.crm_prod_info (
	prd_id INT,	
	prd_key VARCHAR(30),	
	prd_nm VARCHAR(50),	
	prd_cost INT,	
	prd_line VARCHAR(20),	
	prd_start_dt DATE,	
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL 
	DROP TABLE silver.crm_sales_details
CREATE TABLE silver.crm_sales_details (
	sls_ord_num VARCHAR(20),	
	sls_prd_key VARCHAR(20),	
	sls_cust_id INT,	
	sls_order_dt INT,	
	sls_ship_dt INT,	
	sls_due_dt INT,	
	sls_sales INT,	
	sls_quantity INT,	
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL 
	DROP TABLE silver.erp_cust_az12
CREATE TABLE silver.erp_cust_az12 (
	CID VARCHAR(50),	
	BDATE DATE,	
	GEN VARCHAR(10),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL 
	DROP TABLE silver.erp_loc_a101
CREATE TABLE silver.erp_loc_a101 (
	CID VARCHAR(20),
	CNTRY VARCHAR(20),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL 
	DROP TABLE silver.erp_px_cat_g1v2
CREATE TABLE silver.erp_px_cat_g1v2(
	ID VARCHAR(10),	
	CAT VARCHAR(30),	
	SUBCAT VARCHAR(30),	
	MAINTENANCE VARCHAR(10),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

