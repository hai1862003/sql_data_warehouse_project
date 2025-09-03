/*
---------------------
DATA Transformation & LOAD SILVER
---------------------
PURPOSE:
- This scripts load clean data from Bronze to Silver 
- Using Truncate & Insert

WARNING:
- Running this scripts would truncate all current data in silver schema

*/

----------- START: Insert into [silver].[crm_cust_info] -------------


IF (SELECT COUNT(*) FROM silver.crm_cust_info) != 0
	TRUNCATE TABLE silver.crm_cust_info

INSERT INTO [silver].[crm_cust_info]
           ([cst_id]
           ,[cst_key]
           ,[cst_firstname]
           ,[cst_lastname]
           ,[cst_marital_status]
           ,[cst_gndr]
           ,[cst_create_date]        
		   )
SELECT 
		[cst_id]
      ,[cst_key]
      ,TRIM([cst_firstname]) -- remove leading spaces
      ,TRIM([cst_lastname])  -- remove leading spaces
      , CASE -- map marital_status to full_text
	    WHEN UPPER(TRIM([cst_marital_status] ))= 'M' THEN 'Married'
		WHEN UPPER(TRIM([cst_marital_status] )) = 'S' THEN 'Single'
		ELSE 'N/A'
		END AS cst_marital_status
      ,CASE  -- map gender to full_text
	   WHEN UPPER(TRIM([cst_gndr])) = 'F' THEN 'Female'
	   WHEN  UPPER(TRIM([cst_gndr])) = 'M' THEN 'Male'
	   ELSE 'N/A' 
	   END AS cst_gndr
      ,[cst_create_date]
FROM   -- from crm_cust_info with clean 'cst_id' 
	(SELECT *
	 FROM 
		(
		SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id  ORDER BY cst_create_date Desc) as id_count
		FROM bronze.crm_cust_info
		) as a
		WHERE id_count =1 AND cst_id IS NOT NULL
	) as b;

----------- END: Insert into [silver].[crm_cust_info] -------------
GO
----------- START: Insert into [silver].[crm_prod_info] -------------

-- logic for re-runs
IF (SELECT COUNT(*) FROM silver.crm_prod_info) != 0
	TRUNCATE TABLE silver.crm_prod_info
INSERT INTO [silver].[crm_prod_info]
		   ([prd_id]
		   ,[cat_id]
           ,[prd_key]
           ,[prd_nm]
           ,[prd_cost]
           ,[prd_line]
           ,[prd_start_dt]
           ,[prd_end_dt]
		   )
SELECT 
	  [prd_id] 
	  ,REPLACE(SUBSTRING(prd_key,1,5 ),'-','_') as cat_id
	  ,SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key
      ,TRIM([prd_nm]) as prd_nm
      ,[prd_cost]
      ,CASE -- map abbreviation to Full Name
	  WHEN [prd_line] = 'M' THEN 'Mountain'
	  WHEN [prd_line] = 'S' THEN 'Sport'
	  WHEN [prd_line] = 'R' THEN 'Road' 
	  WHEN [prd_line] = 'T' THEN 'Touring'
	  ELSE 'N/A'
	  END AS prd_line
      ,[prd_start_dt]
      ,DATEADD(day,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) as prd_end_dt
FROM bronze.crm_prod_info;

----------- END: Insert into [silver].[crm_prod_info] -------------
GO

----------- START: Insert into [silver].[crm_sales_details] -------------

-- if data type of date columns from silver.crm_sales_details != date
IF (SELECT COUNT(*) FROM silver.crm_sales_details) != 0
	BEGIN
	TRUNCATE TABLE silver.crm_sales_details
	END;
INSERT INTO [silver].[crm_sales_details]
           ([sls_ord_num]
           ,[sls_prd_key]
           ,[sls_cust_id]
           ,[sls_order_dt]
           ,[sls_ship_dt]
           ,[sls_due_dt]
           ,[sls_sales]
           ,[sls_quantity]
           ,[sls_price]
           )

SELECT TRIM([sls_ord_num])
      ,TRIM([sls_prd_key])
      ,[sls_cust_id]
      ,CASE  -- if valid convert to date, else NULL
		    WHEN len(trim(str([sls_order_dt]))) != 8 THEN NULL
			ELSE CONVERT(date, STR([sls_order_dt]),112)
			END AS [sls_order_dt]
      ,CASE 
		    WHEN len(trim(str([sls_ship_dt]))) != 8 THEN NULL
			ELSE CONVERT(date, STR([sls_ship_dt]),112)
			END AS [sls_ship_dt]
      ,CASE 
		    WHEN len(trim(str([sls_due_dt]))) != 8 THEN NULL
			ELSE CONVERT(date, STR([sls_due_dt]),112)
			END AS [sls_due_dt]
      ,CASE -- if sales = invalid => sales = price*quantity
	WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales!= abs(sls_price)*sls_quantity THEN abs(sls_price)*nullif(sls_quantity,0)
	ELSE sls_sales
	END as sls_sales -- 
      ,[sls_quantity]
      ,CASE -- if price = invalid => price = sales/quantity
	WHEN sls_price IS NULL OR sls_price <=0 THEN abs(sls_sales)/NULLIF(sls_quantity,0)
	ELSE sls_price 
	END as sls_price
  FROM [bronze].[crm_sales_details]
  ;

GO




----------- END: Insert into [silver].[crm_sales_details] -------------


--- START : Insert into [silver].[erp_cust_az12]
IF (SELECT COUNT(*) FROM silver.erp_cust_az12) != 0
	BEGIN
	TRUNCATE TABLE silver.crm_sales_details
	END;

INSERT INTO [silver].[erp_cust_az12]
           ([CID]
           ,[BDATE]
           ,[GEN]
		   )

SELECT 
	   [CID]
      ,[BDATE]
      ,[GEN]

  FROM -- LEFT JOIN silver.crm_cust_info to fill invalid GEN
		(	SELECT 
				CID,
				BDATE,
				CASE 
				WHEN GEN IS NULL OR GEN = '' THEN cst_gndr
				WHEN GEN ='F' THEN 'Female'
				WHEN GEN = 'M' THEN 'Male'
				ELSE GEN
				END AS GEN
			FROM -- TRIM CID to match with cst-key from crm_cust_info
				(
				SELECT
				CASE 
					WHEN LEN(TRIM(CID)) =13  THEN RIGHT(CID,LEN(CID)-3)
					ELSE CID
					END AS CID,
				BDATE,
				GEN
				FROM bronze.erp_cust_az12
				) as t
			LEFT JOIN silver.crm_cust_info as cust
			ON cust.cst_key = t.CID
		) as t2
			

GO

  
     
