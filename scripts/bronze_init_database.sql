/*
Create Database and Schemas

Script Purpose:
This script creates a new database named "DataWarehouse" and three schemas within that 
database: "bronze", "silver", and "gold". These schemas can be used to organize data based on its level of processing or quality, with "bronze" typically representing raw data, "silver" representing cleaned and processed data, and "gold" representing high-quality, curated data ready for analysis.
*/
use master;

CREATE DATABASE DataWarehouse;
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;	
