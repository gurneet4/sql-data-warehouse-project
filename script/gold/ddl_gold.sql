/*
=======================================================================================================
DDL Script: Create Gold Views
=======================================================================================================
Script Purpose:
This script creates views for the gold layer in the data warehouse.
The Gold layer represent the final destination and fact tables(star schema)

Each view performs transformations and combines data from the silver layer to produce
a clean, enriched, and business-ready dataset.

Usaeg:
- These views can be directly queried for analytics and reporting.
=======================================================================================================
*/

-- ==================================================
-- Create gold.dim_customers
-- ==================================================
If object_id('gold.dim_customers', 'V') is not null 
Drop view gold.dim_customers;
go
Create view gold.dim_customers as
Select
	Row_number() over(order by cst_id) customer_key,
	ci.cst_id customer_id,
	ci.cst_key customer_number,
	ci.cst_firstname first_name,
	ci.cst_lastname last_name,
	ci.cst_marital_status marital_status,
	Case when ci.cst_gndr != 'n/a' then ci.cst_gndr
	else coalesce(ca.gen, 'n/a')
	end as gender,
	ci.cst_create_date create_date,
	ca.bdate birthdate,
	la.cntry country
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid;
GO

-- ==================================================
-- Create gold.dim_products
-- ==================================================
If object_id('gold.dim_products', 'V') is not null 
Drop view gold.dim_products;
go
create view gold.dim_products as
Select
  row_number() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
  pn.prd_id product_id,
  pn.prd_key product_number,
  pn.prd_nm product_name,
  pn.cat_id category_id,
  px.cat category,
  px.subcat subcategory,
  px.maintenance,
  pn.prd_cost cost,
  pn.prd_line product_line,
  pn.prd_start_dt start_date
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 px
on pn.cat_id = px.id
where prd_end_dt is null;
GO

-- ==================================================
-- Create gold.dim_products
-- ==================================================
If object_id('gold.fact_sales', 'V') is not null 
Drop view gold.fact_sales;
go
create view gold.fact_sales as
Select
  sd.sls_ord_num order_number,
  gp.product_key,
  cu.customer_key,
  sd.sls_order_dt order_date,
  sd.sls_ship_dt ship_date,
  sd.sls_due_dt due_date,
  sd.sls_sales sales,
  sd.sls_quantity quantity,
  sd.sls_price price
from silver.crm_sales_details sd
left join gold.dim_products gp
on sd.sls_prd_key = gp.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id;
GO
