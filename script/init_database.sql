/*
=========================================================
Create database and schemas
=========================================================
Script Purpose:
This script creates a new database named 'Datawarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database:
'bronze', 'silver', 'gold'.

Warning:
        Running this script will drop the entire 'Datawarehouse' databse if it exists.
        All data in the database will be permanently deleted. Proceed wigth caution and ensure you have proper backups before running this script.
*/

-- Create database' Datawarehourse'
Use master;
-- Drop and recreate 'Datawarehouse' database
If exists (select 1 from sys.databases where name = 'Datawarehouse')
begin
alter database Datawarehouse set single_user with rollback immediate;
drop database datawarehouse;
end;
go

-- Create database
Create database DataWarehouse;
go
Use DataWarehouse;
go
-- Create schemas
Create schema Bronze;
go
Create schema Silver;
go
Create schema Gold;
go

go
