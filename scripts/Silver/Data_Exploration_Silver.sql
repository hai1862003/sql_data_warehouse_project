/*
-----------------------------
Data Exploration
-----------------------------
PURPOSE:
- This scritps checks/explore Data Quality issues from Bronze to load => Silver
- Includes query logic/to filter for clean data 
- Used for Testing: silver tables after inserting

WARNING: 
- Does not contain DDL/DML lanaguage, bust should be run by Query 
- Run by whole cause confusion
*/

-------------------------- START: bronze.crm_cust_info --------------------------

--- Check if (supposed) PK (cst_id) is NULL

-- QUERY: search for null/duplicates 'cst_ids' values
WITH  invalid_cst_ids as 
(
SELECT cst_id, Count(*) as count_id
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) >1 OR cst_id IS NULL
)
-- select all duplicate 'cst_ids' in bronze.crm_cust_info
SELECT *
FROM bronze.crm_cust_info as cust_info
WHERE EXISTS( SELECT 1 FROM  invalid_cst_ids as invalid WHERE invalid.cst_id = cust_info.cst_id)
ORDER BY cst_id asc, cst_create_date asc;


-- >> diffrerence_in_Date between duplicate 'cst_id' => keep only closet date

GO
--  QUERY: select rows with: closet_date  & non-null 'cst_ids' 
SELECT *
FROM
(
	SELECT *, ROW_NUMBER() OVER(PARTITION BY cst_id  ORDER BY cst_create_date Desc) as id_count
	FROM bronze.crm_cust_info
) as a
WHERE id_count =1 AND cst_id IS NOT NULL;

GO

--QUERY: Check for Nulls in FK (cst_key)
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key IS  NULL;
-- >> No Nulls in cst_key


GO
--- QUERY: Check for blank_space in name/string column
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);
-- >> leading spaces in cst_firstname & cst_lastname >> TRIM() 

GO
-- QUERY: Check for values in cst_marital_status & cst_gndr
SELECT DISTINCT cst_marital_status  
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;
-- >> BOTH have NULL. turn abbrivation => full_name?

--------------------------- END: bronze.crm_cust_info -----------------------
GO


-------------------------- START: bronze.crm_prod_info --------------------------

-- Q: Is prd_id PK? is it valid (no Null/duplicates)

SELECT prd_id, Count(*) as count_id
FROM bronze.crm_prod_info
GROUP BY prd_id
HAVING COUNT(*) >1 OR prd_id IS NULL;
-- Valid Result <=> query returns nothing

-- >> A: Query returns nothings. prd_id is valid PK


---- Q: check prd_key (FK) quality 

-- check prd_key for NULL/blank space
SELECT *
FROM bronze.crm_prod_info
WHERE prd_key IS NULL 
      OR prd_key != TRIM(prd_key);

---Q: is prd_key - prd_nm: transitive dependencies?
-- for each prd_key,  is it a distinct prd_nm?
SELECT prd_key
FROM bronze.crm_prod_info
GROUP BY prd_key
HAVING count(distinct(prd_nm)) >1
-- Valid Result <=> return nothings


--- Q: DERIVED COL: prd_cat (1st 5 char of "prd_key") to match erp_px_cat_g1v2  
SELECT prd_key, REPLACE(SUBSTRING(prd_key,1,5 ),'-','_') as prd_cat 
FROM bronze.crm_prod_info

-- QUERY RETURNS check

--- Q: derived col: prd_key_2 (from prd_key) MATCH crm_sales_details.sls_prd_key
SELECT prd_key, SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key_match
FROM bronze.crm_prod_info
-- QUERY RETURNS check


-- Check for invalid prd_cost (negative/null)
SELECT *
FROM bronze.crm_prod_info
WHERE prd_cost <0 or prd_cost IS NULL;
-- QUERY Returns: invalid cost entries

-- prd_line: check values (low-cardiniality)
SELECT distinct(prd_line)
FROM bronze.crm_prod_info;
-- Valid result <=> empty

-- 'prd_start_dt/prd_end_dt': check for NULL  
SELECT *
FROM bronze.crm_prod_info
WHERE prd_start_dt IS NULL;
-- Valid Returns = nothing

---"'prd_start_dt, prd_end_dt': Check for valid (start_dt < end_dt)
SELECT *, ROW_NUMBER() OVER(PARTITION BY prd_key ORDER BY prd_start_dt asc) as flag
FROM bronze.crm_prod_info
WHERE prd_start_dt < prd_end_dt
      OR prd_end_dt IS NULL;
-- QUERY RETURNS: Nothings <=> all invalid rows (start_dt > end_dt)

--- 'prd_start_dt, prd_end_dt': Check for invalid rows (start_dt > end_dt)  
SELECT *, ROW_NUMBER() OVER(PARTITION BY prd_key ORDER BY prd_start_dt asc) as flag
FROM bronze.crm_prod_info
WHERE prd_start_dt < prd_end_dt
ORDER BY prd_id asc;
-- QUERY RETURNS: invalid rows

-- FIX: create 'prd_end_dt' being next row 'prd_start_dt' -1
SELECT *, DATEADD(day,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) as prd_end_dt_test
FROM bronze.crm_prod_info; 
-- QUERY Returns: table with new prd_end_dt




-- QUERY RETURN: products with closet date
--------------------------- END: bronze.crm_prod_info -----------------------
GO
-------------------------- START: bronze.crm_sales_details --------------------------
--Q: sls_ord_num  != PK?
SELECT *
FROM
	(
	SELECT *, ROW_NUMBER() OVER(PARTITION BY sls_ord_num ORDER BY sls_ord_num) as duplicate
	FROM bronze.crm_sales_details
	) as a
WHERE duplicate >1;
-- Expectation: QUERY returns <=> sls_ord_num != PK (sls_ord_num is duplicate)

--- Composite PK? sls_ord_num + [?]

-- Q: sls_ord_num + sls_prd_key?
SELECT sls_ord_num
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(distinct(sls_prd_key)) >1;
-- Expectation: Returns Nothing <=>  sls_ord_num + sls_prd_key = PK

-- Q: sls_ord_num + sls_cust_id?
SELECT sls_ord_num
FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(distinct(sls_cust_id)) >1;
-- Expectation: Returns Nothing <=>  sls_ord_num + sls_prd_key = PK
-- >> sls_ord_num + sls_cust_id = PK

-- Q: NULL CHECK:   sls_ord_num , sls_cust_id, sls_prd_key
SELECT sls_ord_num, sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL
      OR sls_cust_id IS NULL
	  OR sls_prd_key IS NULL;
-- Expectation: Returns Nothing <=> no Null in all col

-- Q: blank_space CHECK:   sls_ord_num , sls_prd_key
SELECT sls_ord_num, sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_ord_num  != TRIM(sls_ord_num)
      OR sls_prd_key !=TRIM(sls_prd_key);
-- Expectation: nothing <=> no blankspace in both col

-- CHECK: sls_cust_id NOT IN cst_id(crm_cust_info)
SELECT *
FROM bronze.crm_sales_details s
WHERE NOT EXISTS( 
			SELECT 1 FROM silver.crm_cust_info WHERE cst_id = sls_cust_id
			)
-- QUERY RETURN: Nothing <=> all sls_cust_id is in cst_id

-- CHECK: sls_prd_key NOT IN prd_key(crm_prod_info)
SELECT *
FROM bronze.crm_sales_details s
WHERE NOT EXISTS( 
			SELECT * FROM silver.crm_prod_info WHERE prd_key = sls_prd_key
			);
-- QUERY RETURN:  NOthing <=> all sls_prd_key(crm_sales_details) is in prd_key(crm_prod_info)

-- CHECK Data type of  bronze.crm_sales_details
SELECT c.name as col_name, t.name as data_type,  'bronze.crm_sales_details' as of_table
FROM sys.columns c
LEFT JOIN sys.types t
ON c.user_type_id = t.user_type_id
WHERE c.object_id = OBJECT_ID('bronze.crm_sales_details', 'U')

------ Convert _dt columns to 'date' type
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NULL;
-- check if '%_dt' column got non-date covertible length (!=8)
SELECT  distinct 
	len(TRIM(str(sls_order_dt))) as order_dt_len, 
	len(TRIM(str(sls_ship_dt))) as ship_dt_len, 
	len(TRIM(str(sls_due_dt))) as due_dt_len 
FROM bronze.crm_sales_details;
-- EXPECTATION: len = 8 <=> valid 

-- search for length(str()) != 8  in dt_col
SELECT *
FROM bronze.crm_sales_details
WHERE len(trim(str(sls_order_dt))) != 8


--

-- CHECK: Invalid Date Order (sls_order_dt > sls_ship_dt) OR sls_order_dt > sls_due_dt 
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR
      sls_order_dt > sls_due_dt; 
-- QUERY RETURN: invalid date

-- CHECK: invalid sls_sales, sls_quantity, sls_price
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price,
CASE
	WHEN sls_price IS NULL OR sls_price <=0 THEN abs(sls_sales)/NULLIF(sls_quantity,0) -- if price = invalid, price = sales/quantity
	ELSE sls_price 
	END as sls_price_fix,
CASE 
	WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales!= abs(sls_price)*sls_quantity THEN abs(sls_price)*nullif(sls_quantity,0) --if sales = invalid
	ELSE sls_sales
	END as sls_sales_fix
FROM bronze.crm_sales_details
WHERE sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
      OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	  OR sls_sales != sls_quantity * sls_price;
-- QUERY RETURN: invalid rows


-------------------------- END: bronze.crm_sales_details --------------------------

------------------------- START: bronze.erp_cust_az12-----------------------------

-- bronze.erp_cust_az12.CID = bronze.crm_cust_info.cst_key?
SELECT *
FROM bronze.erp_cust_az12


SELECT *
FROM bronze.crm_cust_info;

-- CID: check quality
SELECT DISTINCT LEN(CID)
FROM bronze.erp_cust_az12;

-- Check for Blankspace: CID
SELECT *
FROM bronze.erp_cust_az12
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

-- GEN: check for values
SELECT DISTINCT GEN
FROM ##clean_CID;
-- QUERY RETURNS: type of values from Gender



-- fix GEN values ==> filling invalid entries with cst_gndr from silver.crm_cust_info
-- check if silver_crm_cust_info contains NULL in cst_gndr
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

-- CHECK: GEN -pre-transform 
SELECT GEN, COUNT(*) as count_val
FROM bronze.erp_cust_az12
GROUP BY GEN;




-- CHECK: ZGEN - post-transform
WITH fix_GEN as 
(
SELECT 
	*,
	CASE 
		WHEN GEN IS NULL OR GEN = '' THEN cst_gndr
		WHEN GEN ='F' THEN 'Female'
		WHEN GEN = 'M' THEN 'Male'
		ELSE GEN
	END AS new_GEN
FROM ##clean_CID az
LEFT JOIN silver.crm_cust_info cust
ON cust.cst_key = az.CID
)

SELECT new_GEN, COUNT(*)
FROM fix_GEN
GROUP BY new_GEN;
-- QUERY EXPECTS: No NULL

-- Check: different genders of customers between: #clean_ID & crm_cust_info
WITH fix_GEN as 
(
SELECT 
*,
CASE 
WHEN GEN IS NULL OR GEN = '' THEN cst_gndr
WHEN GEN ='F' THEN 'Female'
WHEN GEN = 'M' THEN 'Male'
ELSE GEN
END AS new_GEN
FROM ##clean_CID az
LEFT JOIN silver.crm_cust_info cust
ON cust.cst_key = az.CID
)

SELECT *
FROM fix_GEN
WHERE new_GEN != cst_gndr
AND cst_gndr != 'N/A'

------------------------- END: bronze.erp_cust_az12-----------------------------

------------------------- START: bronze.erp_loc_a101-----------------------------

-- Cardiniality Check: CID 
SELECT DISTINCT(LEN(CID)) 
FROM bronze.erp_loc_a101;


-- CHECK CID: string not following %-% format
SELECT *
FROM bronze.erp_loc_a101
WHERE CID NOT LIKE '%-%';

-- CHECK CNTRY: null
SELECT DISTINCT CNTRY
FROM bronze.erp_loc_a101;

-- CNTRY: pre-transofrmation
SELECT DISTINCT(CNTRY)
FROM bronze.erp_loc_a101

-- CHECK: blank spaces
SELECT *
FROM bronze.erp_loc_a101
WHERE CNTRY != TRIM(CNTRY);

-- Data Enrichment Fix
With clean_erp_loc as
(
	SELECT
		CASE 
		WHEN CHARINDEX('-',CID) != 0 THEN TRIM(REPLACE(CID,'-',''))
		ELSE TRIM(CID)
		END as CID -- remove '-' from CID
	   ,CASE
		WHEN CNTRY = 'US' OR CNTRY = 'USA' THEN 'United States'
		WHEN CNTRY = '' OR CNTRY IS NULL OR CNTRY = 'DE' THEN 'N/A'
		ELSE TRIM(CNTRY)
		END AS CNTRY -- Data enrichment for CNTRY
	FROM bronze.erp_loc_a101
)

-- CHECK for mismatches between cleaned_erp_loc_a101 & crm_cust_info
SELECT *
FROM clean_erp_loc clean
FULL OUTER JOIN silver.crm_cust_info cust
ON clean.CID = cust.cst_key
WHERE CID IS NULL 
	  OR cst_key IS NULL;

------------------------- END: bronze.erp_loc_a101-----------------------------


------------------------- START: bronze.erp_px_cat_g1v2-----------------------------
SELECT TOP 1000 *
FROM bronze.erp_px_cat_g1v2;

SELECT TOP 1000 *
FROM silver.crm_prod_info;

-- Col 'ID': check quality 
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE ID IS NULL;

-- mismatches:col 'ID' of erp_px_cat_g1v2  vs silver.crm_prod_info 
SELECT *
FROM bronze.erp_px_cat_g1v2 as g1v2
FULL OUTER JOIN silver.crm_prod_info as prod
ON g1v2.ID = prod.cat_id
WHERE ID IS NULL 
OR cat_id IS NULL;
-- RETURNS: mismatches between 2

-- Col 'CAT': quality check
SELECT DISTINCT CAT
FROM bronze.erp_px_cat_g1v2;






------------------------- END: bronze.erp_px_cat_g1v2-----------------------------