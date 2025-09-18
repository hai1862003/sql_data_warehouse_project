/*
-------------------
Data Quality Test - Silver
---------------------
Purpose:
- This scripts is used to check data quality for tables Silver layer, Specifically:
 +) Nulls/Duplicates/Unwanted Space/Invalid Values
 +) Normalization of Data
 +)

*/
-------------------------- START: silver.crm_cust_info --------------------------

-- QUERY: search for null/duplicates 'cst_ids' values
WITH  invalid_cst_ids as 
(
SELECT cst_id, Count(*) as count_id
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 
)
-- select all duplicate 'cst_ids' in silver.crm_cust_info
SELECT *
FROM silver.crm_cust_info as cust_info
WHERE EXISTS( SELECT 1 FROM  invalid_cst_ids as invalid WHERE invalid.cst_id = cust_info.cst_id) OR cust_info.cst_id IS NULL
ORDER BY cst_id asc, cst_create_date asc;

--QUERY: Check for Nulls in FK (cst_key)
SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key IS  NULL;

--- QUERY: Check for blank_space in name/string column
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
-- >> leading spaces in cst_firstname & cst_lastname >> TRIM() 


-- QUERY: Check for values in cst_marital_status & cst_gndr
SELECT DISTINCT cst_marital_status  
FROM silver.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;
-- >> BOTH have NULL. turn abbrivation => full_name?

-------------------------- END: silver.crm_cust_info --------------------------


-------------------------- START: silver.crm_prod_info --------------------------

-- Q: Is prd_id PK? is it valid (no Null/duplicates)

SELECT prd_id, Count(*) as count_id
FROM silver.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;
-- Valid Result <=> query returns nothing

-- >> A: Query returns nothings. prd_id is valid PK


---- Q: check prd_key (FK) quality 

-- check prd_key for NULL/blank space
SELECT *
FROM silver.crm_prod_info
WHERE prd_key IS NULL 
      OR prd_key != TRIM(prd_key);

---Q: is prd_key - prd_nm: transitive dependencies?
-- for each prd_key,  is it a distinct prd_nm?
SELECT prd_key
FROM silver.crm_prod_info
GROUP BY prd_key
HAVING count(distinct(prd_nm)) >1
-- Valid Result <=> return nothings

-- Check for invalid prd_cost (negative/null)
SELECT *
FROM silver.crm_prod_info
WHERE prd_cost <0 or prd_cost IS NULL;
-- QUERY Returns: invalid cost entries

-- prd_line: check values (low-cardiniality)
SELECT distinct(prd_line)
FROM silver.crm_prod_info;
-- Valid result <=> empty

-- 'prd_start_dt/prd_end_dt': check for NULL  
SELECT *
FROM silver.crm_prod_info
WHERE prd_start_dt IS NULL;
-- Valid Returns = nothing

--- 'prd_start_dt, prd_end_dt': Check for invalid rows (start_dt > end_dt)  
SELECT *, ROW_NUMBER() OVER(PARTITION BY prd_key ORDER BY prd_start_dt asc) as flag
FROM silver.crm_prod_info
WHERE prd_start_dt > prd_end_dt
ORDER BY prd_id asc;
-- QUERY RETURNS: invalid rows

--------------------------- END: silver.crm_prod_info -----------------------


-------------------------- START: silver.crm_sales_details --------------------------

-- sls_ord_num: Check Duplicates
SELECT *
FROM
	(
	SELECT *, ROW_NUMBER() OVER(PARTITION BY sls_ord_num ORDER BY sls_ord_num) as duplicate
	FROM silver.crm_sales_details
	) as a
WHERE duplicate >1;
-- QUERY Returns: Duplicates


-- Q: sls_ord_num + sls_cust_id?
SELECT sls_ord_num
FROM silver.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(distinct(sls_cust_id)) >1;
-- Expectation: Returns Nothing <=>  sls_ord_num + sls_prd_key = PK


-- Q: NULL CHECK:   sls_ord_num , sls_cust_id, sls_prd_key
SELECT sls_ord_num, sls_cust_id
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL
      OR sls_cust_id IS NULL
	  OR sls_prd_key IS NULL;
-- Expectation: Returns Nothing <=> no Null in all col

-- Q: blank_space CHECK:   sls_ord_num , sls_prd_key
SELECT sls_ord_num, sls_prd_key
FROM silver.crm_sales_details
WHERE sls_ord_num  != TRIM(sls_ord_num)
      OR sls_prd_key !=TRIM(sls_prd_key);
-- Expectation: nothing <=> no blankspace in both col

-- CHECK: sls_cust_id NOT IN cst_id(crm_cust_info)
SELECT *
FROM silver.crm_sales_details s
WHERE NOT EXISTS( 
			SELECT 1 FROM silver.crm_cust_info WHERE cst_id = sls_cust_id
			)
-- QUERY RETURN: Nothing <=> all sls_cust_id is in cst_id

-- CHECK: sls_prd_key NOT IN prd_key(crm_prod_info)
SELECT *
FROM silver.crm_sales_details s
WHERE NOT EXISTS( 
			SELECT * FROM silver.crm_prod_info WHERE prd_key = sls_prd_key
			);
-- QUERY RETURN:  NOthing <=> all sls_prd_key(crm_sales_details) is in prd_key(crm_prod_info)

-- CHECK: Invalid Date Order (sls_order_dt > sls_ship_dt/ sls_due_dt OR Current_date)
SELECT sls_order_dt, sls_ship_dt, sls_due_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR
      sls_order_dt > sls_due_dt OR
	  sls_order_dt > GETDATE(); 
-- QUERY RETURNS: ORDER DATE



-- CHECK: invalid sls_sales, sls_quantity, sls_price
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
      OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	  OR sls_sales != sls_quantity * sls_price;
-------------------------- END: silver.crm_sales_details --------------------------

------------------------- START: silver.erp_cust_az12-----------------------------
-- CID: check quality
SELECT DISTINCT LEN(CID)
FROM silver.erp_cust_az12;

-- Check for Blankspace: CID
SELECT *
FROM silver.erp_cust_az12
WHERE CID != TRIM(CID);


-- CHECK for mismatches between clean_CID and crm_cust_info
SELECT *
FROM silver.crm_cust_info cust
FULL OUTER JOIN ##clean_CID clean -- ##clean_CID is a temp table ('erp_cust_az12'   with trimmed 'CID' for Joining)
ON cust.cst_key = clean.CID
WHERE CID IS NULL OR
      cst_key IS NULL;


-- BDATE: check for nulls
SELECT BDATE
FROM ##clean_CID
WHERE BDATE IS NULL;


-- CHECK: GEN values 
SELECT GEN, COUNT(*) as count_val
FROM silver.erp_cust_az12
GROUP BY GEN;

------------------------- END: silver.erp_cust_az12-----------------------------

------------------------- START: silver.erp_loc_a101-----------------------------
-- Cardiniality Check: CID 
SELECT DISTINCT(LEN(CID)) 
FROM silver.erp_loc_a101;


-- CHECK CID: invalid values (contains "-")"
SELECT *
FROM silver.erp_loc_a101
WHERE CID  LIKE '%-%';
-- QUERY RETURNS: invalid values

-- CHECK CNTRY: null
SELECT DISTINCT CNTRY
FROM silver.erp_loc_a101;


-- CHECK: blank spaces
SELECT *
FROM silver.erp_loc_a101
WHERE CNTRY != TRIM(CNTRY);

------------------------- END: silver.erp_loc_a101-----------------------------

------------------------- START: silver.erp_px_cat_g1v2-----------------------------
-- Col 'ID': check quality 
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE ID IS NULL;

-- mismatches:col 'ID' of erp_px_cat_g1v2  vs silver.crm_prod_info 
SELECT *
FROM silver.erp_px_cat_g1v2 as g1v2
FULL OUTER JOIN silver.crm_prod_info as prod
ON g1v2.ID = prod.cat_id
WHERE ID IS NULL 
OR cat_id IS NULL;
-- RETURNS: mismatches between 2

-- Col 'CAT': quality check
SELECT DISTINCT CAT
FROM silver.erp_px_cat_g1v2;

------------------------- END: silver.erp_px_cat_g1v2-----------------------------