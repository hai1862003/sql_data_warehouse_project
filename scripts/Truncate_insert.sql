/* 
-------
TRUNCATE AND INSERT
-------
Purpose: This scripts TRUNCATE AND  (BULK) INSERT tables in bronze layer
         


WARNING: running this will truncate all exisiting data from the table and reload with data from source system
*/

-- INSERT crm_cust_info
TRUNCATE TABLE bronze.crm_cust_info
GO 
BULK INSERT bronze.crm_cust_info 
FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW= 2, -- 1 is header
	FIELDTERMINATOR = ',',
	TABLOCK
)

GO
-- INSERT crm_prod_info
TRUNCATE TABLE bronze.crm_prod_info
GO 
BULK INSERT bronze.crm_prod_info
FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW= 2, -- 1 is header
	FIELDTERMINATOR = ',',
	TABLOCK
);

GO
-- INSERT crm.sales_details 
TRUNCATE TABLE bronze.crm_sales_details
GO 
BULK INSERT bronze.crm_sales_details
FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW= 2, -- 1 is header
	FIELDTERMINATOR = ',',
	TABLOCK
);


GO
-- INSERT erp_cust_az12
TRUNCATE TABLE bronze.erp_cust_az12
GO 
BULK INSERT bronze.erp_cust_az12
FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
	FIRSTROW= 2, -- 1 is header
	FIELDTERMINATOR = ',',
	TABLOCK
);
GO
-- INSERT erp_loc_a101
TRUNCATE TABLE bronze.erp_loc_a101
GO 
BULK INSERT bronze.erp_loc_a101
FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
	FIRSTROW= 2, -- 1 is header
	FIELDTERMINATOR = ',',
	TABLOCK
);

GO
-- INSERT erp_px_cat_g1v2
TRUNCATE TABLE bronze.erp_px_cat_g1v2
GO 
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
	FIRSTROW= 2, -- 1 is header
	FIELDTERMINATOR = ',',
	TABLOCK
);


