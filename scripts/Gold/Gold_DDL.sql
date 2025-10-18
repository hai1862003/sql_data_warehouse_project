
/*
-- Gold Layer: create business objects

- PURPOSE: view-tables  of business objects from Silver layer



*/

-- create view: Customer

CREATE OR ALTER VIEW gold.dim_customer AS
	(SELECT 
			ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, --surrogate key
			ci.cst_id as customer_id,
			ci.cst_key as customer_number,
			ci.cst_firstname as first_name,
			ci.cst_lastname as last_name,
			la.CNTRY as country,
			ci.cst_marital_status as marital_status,
			CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr -- take cst_gndr value if not null, else ca.gen
				ELSE COALESCE(ca.GEN,'N/A') 
				END AS gender,
			ca.BDATE as birthdate,
			ci.cst_create_date as create_date	
		FROM silver.crm_cust_info ci
		LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.CID
		LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.CID
	)	
;

GO

-- create view: PRODUCT
CREATE OR ALTER VIEW gold.dim_product as
	SELECT 
		  ROW_NUMBER() OVER(ORDER BY prd_inf.prd_start_dt, prd_inf.prd_key) as product_key --surrogate key
		  ,prd_inf.[prd_id] as product_id
		  ,prd_inf.[prd_key] as product_number
		  ,prd_inf.[prd_nm] as product_name
		  ,prd_inf.[cat_id] as category_id
		  ,prd_mnt.[CAT] as product_category
		  ,prd_mnt.[SUBCAT] as product_subcategory
		  ,prd_inf.[prd_line] as product_line
		  ,prd_mnt.[MAINTENANCE] as maintenance
		  ,prd_inf.[prd_cost] as product_cost
		  ,prd_inf.[prd_start_dt] as price_start_dt	  
	FROM [silver].[crm_prod_info] as prd_inf
	LEFT JOIN silver.erp_px_cat_g1v2 as prd_mnt
	ON prd_inf.cat_id = prd_mnt.ID
	WHERE prd_inf.prd_end_dt IS NULL -- filter all historial data
;
GO

-- create view: Fact USE [DataWareHouse]

CREATE OR ALTER VIEW gold.fact_sales AS
	SELECT 
		  [sls_ord_num] as order_number
		  ,prd.product_key -- surrogate keys from dim
		  ,cu.customer_key -- surrogate key from dim
		  ,[sls_order_dt] as order_date
		  ,[sls_ship_dt] as shipping_date
		  ,[sls_due_dt] as due_date
		  ,[sls_sales] as sales_amount
		  ,[sls_quantity] as quantity
		  ,[sls_price]  as price
	  FROM [silver].[crm_sales_details] as sd -- join to get surrogate keys from dimensions
	  LEFT JOIN gold.dim_customer as cu
	  ON sd.sls_cust_id = cu.customer_id
	  LEFT JOIN gold.dim_product as prd
	  ON sd.sls_prd_key = prd.product_number

GO


