/* 
-------
TRUNCATE AND INSERT
-------
Purpose: This scripts creates a Procedure  that: 
       +) TRUNCATE AND  (BULK) INSERT tables in bronze layer.
	   +) Log Load_duration of each tables & whole layer 	
      
WARNING: running this will truncate all exisiting data from the table and reload with data from source system
*/

-- INSERT crm_cust_info

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_layer DATETIME,@end_time_layer DATETIME  ; -- declear var to capture start/end runtime
	BEGIN TRY
		SET @start_time_layer = GETDATE()
		-- Print for console
		PRINT '-----------------------'
		PRINT  'Loading Bronze Layer'
		PRINT  '-----------------------'


		PRINT '------------------'
		PRINT 'Loading CRM tables'
		PRINT '------------------'

		-- INSERT crm_cust_info
		PRINT 'Truncate & Insert bronze.crm_cust_info'
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info 
		FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW= 2, -- 1 is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		-- get runtime
		PRINT '>>> LOAD DURATION: '  + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'second'  
		PRINT '------------------------------------'

		
		PRINT 'Truncate & Insert bronze.crm_prod_info'
		-- INSERT crm_prod_info
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_prod_info
		BULK INSERT bronze.crm_prod_info
		FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW= 2, -- 1 is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>> LOAD DURATION: '  + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'second'  
		PRINT '------------------------------------'

		-- INSERT crm_sales_details
		
		PRINT 'Truncate & Insert bronze.crm_sales_details'
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW= 2, -- 1 is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>> LOAD DURATION: '  + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'second'  
		PRINT '------------------------------------'

		PRINT '------------------'
		PRINT 'Loading ERP tables'
		PRINT '------------------'

		-- INSERT erp_cust_az12
		SET @start_time = GETDATE()
		PRINT 'Truncate & Insert bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW= 2, -- 1 is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>> LOAD DURATION: '  + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'second'  
		PRINT '------------------------------------'
		
		-- INSERT erp_loc_a101

		PRINT 'Truncate & Insert bronze.erp_loc_a101'
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW= 2, -- 1 is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>> LOAD DURATION: '  + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'second'  
		PRINT '------------------------------------'
		
		
		-- INSERT erp_px_cat_g1v2
		PRINT 'Truncate & Insert bronze.erp_px_cat_g1v2'
		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\SQL Data_course_mat\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW= 2, -- 1 is header
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>> LOAD DURATION: '  + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + 'second'  
		PRINT '------------------------------------'
		
		SET @end_time_layer = GETDATE()
		PRINT '>>>>> TOTAL DURATION: ' + CAST(DATEDIFF(second, @start_time_layer,@end_time_layer) AS NVARCHAR) + 'second'

	END TRY
	BEGIN CATCH
		PRINT '===================================='
		PRINT 'Error Occured During Bronze Layer'
		PRINT 'Error Message' + ERROR_MESSAGE()
		PRINT 'Error Message' + Cast(ERROR_NUMBER() AS NVARCHAR)
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR)
 		PRINT '===================================='
	END CATCH
END
	
GO

EXEC bronze.load_bronze
