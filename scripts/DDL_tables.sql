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

CREATE TABLE crm_cust_info ( 
	cst_id INT,
	cst_key VARCHAR(20),
	cst_firstname VARCHAR(20),
	cst_lastname VARCHAR(20),
	cst_marital_status VARCHAR(5),
	cst_gndr VARCHAR(5), 
	cst_create_date DATE  -- must check compatible date format
); 