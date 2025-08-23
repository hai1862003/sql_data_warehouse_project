/*
----------------------------------------------
DATA EXPLORATION 
----------------------------------------------

PURPOSE:
Explore Data from Bronze layer before transformation in Silver
for Data Integration Table



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


-- bronze.crm_sales_details: Why is sls_ord_num not PK? (duplicate ord_num)?
With unique_ord_num as
(SELECT  distinct(a.sls_ord_num)
	FROM
		(
		SELECT *, ROW_NUMBER() OVER(PARTITION BY SLS_ORD_NUM ORDER BY sls_ord_num) as duplicate
		FROM bronze.crm_sales_details
		) as a
	WHERE duplicate >1
)
-- duplicate ord_num => ? sls_ord_num + sls_cust_id = PK?
SELECT u.sls_ord_num 
FROM unique_ord_num u
LEFT JOIN bronze.crm_sales_details s
ON u.sls_ord_num = s.sls_ord_num
GROUP BY u.sls_ord_num
HAVING COUNT(distinct(sls_cust_id)) >1; 
-- >> No sls_ord_num have >1 sls_cust_id ==> PK = sls_ord_num + sls_cust_id

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

