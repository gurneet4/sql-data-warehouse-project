/*
===================================================================================================
Stored Procedure: Load silver layer (bronze>silver)
===================================================================================================
Purpose:
This procedure performs the ETL (Extract, Transform, Load) process to populate the 
'silver' schema tables from the 'bronze' schema.
Actions Performed:
        - Truncates silver tables.
        - Inserts transformed and cleaned data from bronze into silver tables.

Parameters:
This procedure does not accept any parameters.

Usage Example:
exec silver.load_silver;
==================================================================================================
*/


Create or alter procedure silver.load_silver as
begin
declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
begin try
set @batch_start_time = getdate();
Print '================================'
print 'Loading Silver layer'
Print '================================'

Print '--------------------------------'
Print 'Loading CRM Tables'
Print '--------------------------------'
 
    --crm_cust_info table
set @start_time = getdate()
print '>> Truncating table silver.crm_cust_info';
truncate table silver.crm_cust_info;
print '>> Inserting data into: silver.crm_cust_info';
    insert into silver.crm_cust_info(cst_id, cst_key, cst_firstname,
    cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    Select cst_id,
    cst_key,
    trim(cst_firstname) cst_firstname,
    trim(cst_lastname) cst_lastname,
    case when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
    when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
    else 'n/a'
    end cst_marital_status,
    case when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
    when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
    else 'n/a'
    end cst_gndr,
    cst_create_date
    from (select*, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
    from bronze.crm_cust_info where cst_id is not null)t
    where flag_last = 1;
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

    -- prd_info table
set @start_time = getdate()
print '>> Truncating table silver.crm_prd_info';
truncate table silver.crm_prd_info;
print '>> Inserting data into: silver.crm_prd_info';
    insert into silver.crm_prd_info(
    prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    Select prd_id,
    replace(SUBSTRING(prd_key,1,5), '-', '_') cat_id,
    SUBSTRING(prd_key,7, len(prd_key)) as prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    case UPPER(TRIM(prd_line)) 
    when 'S' then 'Other Sales'
    when 'M' then 'Mountain'
    when 'R' then 'Road'
    when 'T' then 'Touring'
    else 'n/a'
    end as prd_line,
    prd_start_dt,
    DATEADD(DAY, -1, lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt
    from bronze.crm_prd_info
    where SUBSTRING(prd_key,7, len(prd_key)) in (
    select sls_prd_key from bronze.crm_sales_details);
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

    -- sales_details table
set @start_time = getdate()
print '>> Truncating table silver.crm_sales_details';
truncate table silver.crm_sales_details;
print '>> Inserting data into: silver.crm_sales_details';
    insert into silver.crm_sales_details(sls_ord_num,sls_prd_key, sls_cust_id, sls_order_dt,
    sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    select sls_ord_num, sls_prd_key, sls_cust_id, 
    case 
    when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
    else cast(cast(sls_order_dt as varchar) as date)
    end as sls_order_dt,
    case 
    when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
    else cast(cast(sls_ship_dt as varchar) as date)
    end as sls_ship_dt, 
    case 
    when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
    else cast(cast(sls_due_dt as varchar) as date)
    end as sls_due_dt, 
    case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity*abs(sls_price)
    then sls_quantity*abs(sls_price)
    else sls_sales
    end as sls_sales,
    sls_quantity,
    case when sls_price is null or sls_price <=0
    then sls_sales/nullif(sls_quantity, 0)
    else sls_price
    end as sls_price
    from bronze.crm_sales_details;
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

Print '--------------------------------'
Print 'Loading ERP Tables'
Print '--------------------------------'

    -- erp_cust_az12 table
set @start_time = getdate()
print '>> Truncating table silver.erp_cust_az12';
truncate table silver.erp_cust_az12;
print '>> Inserting data into: silver.erp_cust_az12';
    insert into silver.erp_cust_az12(cid, bdate, gen)
    Select 
    case when cid like 'NAS%'
    Then substring(cid, 4, len(cid))
    else cid
    end cid,
    case when bdate>getdate() then null
    else bdate
    end as bdate,
    case when upper(trim(gen)) IN ('F', 'FEMALE') THEN 'Female'
    when upper(trim(gen)) IN ('M', 'MALE') THEN 'Male'
    else 'n/a'
    end as gen
    from bronze.erp_cust_az12
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

    -- erp.loc_a101 table
set @start_time = getdate()
print '>> Truncating table silver.erp_loc_a101';
truncate table silver.erp_loc_a101;
print '>> Inserting data into: silver.erp_loc_a101';
    insert silver.erp_loc_a101(cid,cntry)
    select
    replace(cid,'-','') cid,
    case 
    when trim(cntry) = 'DE' then 'Germany'
    when UPPER(trim(cntry)) in ('USA', 'US') then 'United States'
    when trim(cntry) = '' or cntry is null then 'n/a'
    else trim(cntry)
    end as cntry
    from bronze.erp_loc_a101;
set @end_time = getdate()
print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

    -- erp.px_cat_g1v2 table
set @start_time = getdate()
print '>> Truncating table silver.erp_px_cat_g1v2';
truncate table silver.erp_px_cat_g1v2;
print '>> Inserting data into: silver.erp_px_cat_g1v2';
    insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
    select
    id,
    cat,
    subcat,
    maintenance
    from bronze.erp_px_cat_g1v2
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
GO

