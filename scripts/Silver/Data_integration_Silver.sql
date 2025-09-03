/*
----------------------------------------------
DATA INTEGRATION - RELATION
----------------------------------------------

PURPOSE:
- Explore Data relation of Bronze Layer before clean&load to Silver layer



FINDINGS:  
- Data Integration Table
- bronze.erp_cust_az12.CID = bronze.crm_cust_info.cst_key 
  +) bronze.erp_cust_az12.CID: data transform needed (Some Value Are "NSA[cst_key]", some are "[Cst_key]"
- bronze.erp_loc_a101.CID = bronze.crm_cust_info.cst_key
  +) bronze.crm_cust_info.cst_key: values of inconsistent lengths

WARNING: 
- Should be run queries/section by section (not as a whole)
*/

USE DataWareHouse;


SELECT TOP 1000 * 
FROM bronze.crm_cust_info;

SELECT TOP 1000 * 
FROM bronze.crm_prod_info;


-- bronze.crm_sales_details: is sls_ord_num not PK? (duplicate ord_num)?
SELECT *
FROM
	(
	SELECT *, ROW_NUMBER() OVER(PARTITION BY sls_ord_num ORDER BY sls_ord_num) as duplicate
	FROM bronze.crm_sales_details
	) as a
WHERE duplicate >1;
-- RESULT: QUERY RETURNS <=> sls_ord_num is duplicated



-- check if CID is PK
SELECT Count(distinct(CID)), COUNT(*), Count(distinct(CID)) - COUNT(*) as sub
FROM bronze.erp_cust_az12;


-- check if CID is PK
SELECT Count(distinct(CID)), COUNT(*), Count(distinct(CID)) - COUNT(*) as sub
FROM bronze.erp_loc_a101;

-- check if CID is PK
SELECT Count(distinct(ID)), COUNT(*), Count(distinct(ID)) - COUNT(*) as sub
FROM bronze.erp_px_cat_g1v2;

-- bronze.erp_cust_az12:IS bronze.erp_cust_az12.CID = bronze.crm_cust_info.cst_key?
SELECT *
FROM bronze.erp_cust_az12 e
FULL JOIN bronze.crm_cust_info c
ON c.cst_key = RIGHT(e.CID, len(e.CID)-3)
WHERE (c.cst_key IS NULL 
      OR e.CID IS NULL);
-->> YES: but bronze.erp_cust_az12.CID needs string-stripping. some value is "NSA[cust_key]", some is "[cust_key]"

-- IS bronze.erp_loc_a101.CID = bronze.crm_cust_info.cst_key
SELECT *
FROM bronze.erp_loc_a101; 

SELECT  *
FROM bronze.crm_cust_info as cus
FULL JOIN bronze.erp_loc_a101 as cus_loc
ON cus.cst_key = REPLACE(cus_loc.CID,'-','')
WHERE cus.cst_key IS NULL
      OR cus_loc.CID IS NULL;
-- >> YES: but Unclear value  on bronze.crm_cust_info.cst_key?

