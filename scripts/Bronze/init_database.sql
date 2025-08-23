/*
---------
Create Database and Schema
------------------
Purpose: 
	This scripts create Database 'DataWareHouse' along with 3 schema 'bronze', 'silver' and 'gold'.
	Script will drop + recreate  'DataWareHouse' if already exists.

WARNING:
      Running this will drop the 'DatWareHouse'. 
	  All data in it will be deleted.
	  !!!!!!PROCEED WITH CAUTION!!!!!!!!!!!!!!!
*/
-- create new Database "DataWarehouse"
USE master; -- master db used to create new db
IF EXISTS(SELECT * FROM sys.databases WHERE name = 'DataWareHouse')
	BEGIN
		ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE DataWareHouse;
	END
 
CREATE DATABASE DataWareHouse;

GO 

USE DataWareHouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
