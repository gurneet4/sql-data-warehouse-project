/*
======================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
       This procedure loads data into the 'bronze' schema from external csv files.
      It performs the following actions:
      - Truncates the bronze tables before loading data.
      - Uses the 'Bulk Insert' command to load data from CSV files to bronze tables.

Parameters:
    This procedure does not accept any parameters or return any values.

Usage Example:
    Exec bronze.load_bronze;
=====================================================================================================
*/

Create or alter procedure bronze.load_bronze as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
begin try
set @batch_start_time = getdate();
Print '================================'
print 'Loading bronze layer'
Print '================================'

Print '--------------------------------'
Print 'Loading CRM Tables'
Print '--------------------------------'

set @start_time = getdate() 
Print '>>Truncating Table: bronze.crm_cust_info'
truncate table bronze.crm_cust_info;
	Bulk insert bronze.crm_cust_info
	from 'C:\Users\gurneet kaur\OneDrive\Desktop\SQL Databases\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

set @start_time = getdate() 
Print '>>Truncating Table: bronze.crm_prd_info'
truncate table bronze.crm_prd_info;
	Bulk insert bronze.crm_prd_info
	from 'C:\Users\gurneet kaur\OneDrive\Desktop\SQL Databases\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

set @start_time = getdate() 
Print '>>Truncating Table: bronze.crm_sales_details'
truncate table bronze.crm_sales_details;
	Bulk insert bronze.crm_sales_details
	from 'C:\Users\gurneet kaur\OneDrive\Desktop\SQL Databases\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

Print '--------------------------------'
Print 'Loading ERP Tables'
Print '--------------------------------'

set @start_time = getdate() 
Print '>>Truncating Table: bronze.erp_cust_az12'
truncate table bronze.erp_cust_az12;
	Bulk insert bronze.erp_cust_az12
	from 'C:\Users\gurneet kaur\OneDrive\Desktop\SQL Databases\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

set @start_time = getdate() 
Print '>>Truncating Table: bronze.erp_loc_a101'
truncate table bronze.erp_loc_a101;
	Bulk insert bronze.erp_loc_a101
	from 'C:\Users\gurneet kaur\OneDrive\Desktop\SQL Databases\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';


set @start_time = getdate() 
Print '>>Truncating Table: bronze.erp_px_cat_g1v2'
truncate table bronze.erp_px_cat_g1v2;
	Bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\gurneet kaur\OneDrive\Desktop\SQL Databases\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	with(
	firstrow = 2,
	fieldterminator = ',',
	tablock
	);
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
 set @batch_end_time = getdate()

	end try
	begin catch
	Print '================================'
	print 'error occured during loading bronze layer'
	print 'error message' + error_message();
	print 'error message' + cast(error_number() as nvarchar);
	print 'error message' + cast(error_state() as nvarchar);
	print '================================'
	end catch

end 
