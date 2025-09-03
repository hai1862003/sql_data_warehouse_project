/*
---- TEMP TABLES SILVER

Purpose : this scripts creates temp-tables for Data Explorations to Load into Silver Layer

- Run this before Data_exploration_Silver.sql for full functionality


-Warning: Running this scripts will create local temps


*/

-- create local temp_table: #clean_CID ('erp_cust_az12' but with trimmed 'CID' for JOINs w 'crm_cust_info')  
-- If already exists, drop table
USE tempdb;
GO
IF OBJECT_ID('#clean_CID') IS NOT NULL
DROP TABLE #clean_CID;

-- IF not, create temp
USE DataWareHouse;
GO 

WITH new_CID as 
(
SELECT
CASE 
WHEN LEN(TRIM(CID)) =13  THEN RIGHT(CID,LEN(CID)-3)
ELSE CID
END AS CID,
BDATE,
GEN
FROM bronze.erp_cust_az12
)
SELECT *
INTO #clean_CID
FROM new_CID;