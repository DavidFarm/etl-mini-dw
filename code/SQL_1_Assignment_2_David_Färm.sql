-- -----------------------------------
-- SQL 1 DE25 Assignment 2 David Färm
-- -----------------------------------

-- --------------------------------------------------------------------------
--		TABLE OF CONTENT
--		Code overview
-- --------------------------------------------------------------------------
-- Stage 1 Setup db, schemas, views
-- (Requirements 1)
-- (VG for etl_load_tracker and etl_run_log)
-- General: active_from, active_to and is_current dim columns for SCD2 logic
--		Stage 1.1 - Database
--		Stage 1.2 - Schemas
--		Stage 1.3 - DimDate
--		Stage 1.4 - etl_load_tracker
--		Stage 1.5 - DimCustomer
--			Stage 1.5.1 - DimCustomer (delta view)
--			Stage 1.5.2 - DimCustomer (active snapshot)
--			Stage 1.5.3 - Create table DimCustomer
--		Stage 1.6 - DimSalesPerson
--			Stage 1.6.1 - DimSalesPerosn (delta view)
--			Stage 1.6.2 - DimSalesPerson (active snapshot)
--			Stage 1.6.3 - Create table DimSalesPerson
--		Stage 1.7 - DimProduct
--			Stage 1.7.1 - DimProduct (delta view)
--			Stage 1.7.2 - Dimproduct (active snapshot)
--			Stage 1.7.3 - Create table DimProduct
--		Stage 1.8 - FactSales
--			Stage 1.8.1 - Create source view
--			Stage 1.8.2 - Create table FactSales
--		Stage 1.9 - etl_run_log
-- --------------------------------------------------------------------------
-- Stage 2 Script to load historical data into the tables you created
-- (Requirements 2)
--		Stage 2.1 - DimCustomer
--		Stage 2.2 - DimDate
--		Stage 2.3 - DimSalesPerson
--		Stage 2.4 - DimProduct
--		Stage 2.5 - FactSales
-- --------------------------------------------------------------------------
-- Stage 3 Script to append new data to the tables
-- (Requirements 3)
--		Stage 3.1.1 - Create sp for DimCustomer
--		Stage 3.1.2 - Run sp for DimCustomer
--		Stage 3.2.1 - Create sp for DimSalesPerson
--		Stage 3.2.2 - Run sp for DimSalesPerson
--		Stage 3.3.1 - Create sp for DimProduct
--		Stage 3.3.2 - Run sp for DimProduct
--		Stage 3.4.1 - Create sp for FactSales
--		Stage 3.4.2 - Run sp for FactSales
-- --------------------------------------------------------------------------
-- Stage 4 Optional simplified mart views for BI/Analytics
-- This a small, barebone stage to illustrate concept since it is not in REQUIREMENTS
--		Stage 4.1 - DimCustomer
--		Stage 4.2 - DimSalesPerson
--		Stage 4.3 - DimProduct
--		Stage 4.4 - FactSales
-- --------------------------------------------------------------------------
-- Stage 5 Testing ids/refs/etc for validations
-- After a full run, scroll here and run these if you want.
--		Stage 5.1 Check full table structure
--		Stage 5.2 Check DimDate
--		Stage 5.3 Check DimCustomer
--		Stage 5.4 Check DimSalesPerson
--		Stage 5.5 Check DimProduct
--		Stage 5.6 Check FactSales
--		Stage 5.7 Check load tracker
-- --------------------------------------------------------------------------
--		Stage 5.8 Check logs
--		NOTE: This is for checking this code 
--		Run F5 as may times as you like and scroll to last result every time 
--		There *should* only be 1 1st time per table and all runs should report SUCCESS
-- --------------------------------------------------------------------------
-- Stage 6 Scheduling and jobs
--		Suggested run order with code 
--		Suggested testing practise 
-- --------------------------------------------------------------------------
-- Stage X Sanity checks for code
-- Not in REQUIREMENTS - but important
--		Check imported totals against source totals 
-- --------------------------------------------------------------------------


-- ------------------------------------------------------------------
-- STAGE 1 / REQUIREMENT 1
-- Setup database, schemas and views
-- NOTE: Many views will COLLATE source data to destination format
--		This is to avoid problems with string functions
-- NOTE: Views use > '9999-12-30' to avoid time (incl nanosec) problems
-- ------------------------------------------------------------------

-- STAGE 1.1 - Database
-- Sizes: Starting low, main db (WideWorldImporters) is 1+2GB which seems like an overkill
-- Reason: This db, and all the others, are running on my personal laptop!
-- AdWorks 2019 is about 400mb and AdWorksDW2019 is about 220mb
-- exec sp_helpdb 'WWI_DW';
-- Started at 180 after all dims, about 300 after factSales, 600 at full load (max once)
-- Log extends to 100m after some runs
-- DECISION: Start db at 512, but allow up to 2gig, log at 256 to 1g (if bigger fact tables)
-- NOTE: Added dynamic filepaths to allow for use on independent systems

if DB_ID('WWI_DW') is null
begin
    declare @datapath nvarchar(260) = cast(serverproperty('instancedefaultdatapath') as nvarchar(260));
    declare @logpath  nvarchar(260) = cast(serverproperty('instancedefaultlogpath')  as nvarchar(260));

    declare @sql nvarchar(max) = N'
    create database WWI_DW
    on primary
    (
        name = N''WWI_DW'',
        filename = N''' + @datapath + 'wwi_dw.mdf'',
        size = 512mb,
        maxsize = 2048mb,
        filegrowth = 64mb
    )
    log on
    (
        name = N''WWI_DW_LOG'',
        filename = N''' + @logpath + 'wwi_dw_log.ldf'',
        size = 256mb,
        maxsize = 1024mb,
        filegrowth = 64mb
    );';

    exec (@sql);
end;
GO

--Switch to correct db after creation
USE WWI_DW;
GO

-- STAGE 1.2 - Schemas
-- -------------------
-- NAMING: davidf_ + stage (naming is for assignment evaluation purposes)
-- Staging - main purpose to hold views on source data
-- NOTE: Staging creates views
-- Depending on enviroment, these might ALL be tables (that might be dumped later)
-- I'm doing views for simplicity and to reduce data
if not exists (select 1 from sys.schemas where name = 'davidf_staging')
    EXEC('create schema davidf_staging');
-- Intermediate - main purpose to transform source data and hold fact/dim
-- NOTE: Int creates tables
if not exists (select 1 from sys.schemas where name = 'davidf_int')
    EXEC('create schema davidf_int');
-- Marts - main purpose to show cleaned data to BI/analytics
-- NOTE: Marts create only views
if not exists (select 1 from sys.schemas where name = 'davidf_mart')
    EXEC('create schema davidf_mart');
GO

-- STAGE 1.3.1 - DimDate table
if not exists (select 1 from sys.tables where name = 'DimDate' and schema_id = SCHEMA_ID('davidf_int'))
	create table davidf_int.DimDate(
		date_key			int not null primary key,
		date_value			date not null,
		full_date			nvarchar(255) not null,
		day_of_week_name	nvarchar(50) not null,
		day_of_week_short	nvarchar(50) not null,
		day_of_week_number	int not null,
		day_number_in_month	int not null,
		day_number_in_year	int not null,
		[week]				int not null,
		month_name			nvarchar(50) not null,
		month_name_short	nvarchar(50) not null,
		month_number		int not null,
		year_month			nvarchar(50) not null,
		quarter_name		nvarchar(50) not null,
		quarter_number		int not null,
		year_quarter_name	nvarchar(50) not null,
		year_quarter_number	nvarchar(50) not null,
		year_value			int not null,
		weekday_flag		nvarchar(7) not null
	);
GO

-- STAGE 1.3.2 - DimDate sp
-- sp to recreate DimDate with dates from start to end dates
-- Not meant to be run on schedule, fire and forget style
create or alter procedure createDimDate @startDate DATE = '2016-01-01', @endDate DATE = '2018-12-31', @fiscal_year_start_month INT = 1
AS
BEGIN
	set nocount on;

	declare @start_time datetime2 = SYSUTCDATETIME(), @rows_inserted int = 0;

	--Set language according to REQUIREMENTS
	--Also, let's not forget to change datefirst to match new settings
	SET LANGUAGE Svenska;
	SET DATEFIRST 1;

	-- clear data but do not remove table
	delete from davidf_int.DimDate;

	with dates as (
		select @startdate as datevalue
		union all
		select dateadd(dd, 1, dates.datevalue)
		from dates
		where dates.datevalue < @endDate
	)
	insert into davidf_int.DimDate
	select
		cast(format(datevalue, 'yyyyMMdd') as int) as date_key,
		datevalue							as date_value,
		format(datevalue, 'D')				as full_date,
		datename(dw, datevalue)				as day_of_week_name,
		format(datevalue, 'ddd')			as day_of_week_short,
		datepart(weekday, datevalue)		as day_of_week_number,
		datepart(dd, datevalue)				as day_number_in_month,
		datepart(dy, datevalue)				as day_number_in_year,
		datepart(wk, datevalue)				as week,
		datename(mm, datevalue)				as month_name,
		format(datevalue, 'MMM')			as month_name_short,
		datepart(mm, datevalue)				as month_number,
		format(datevalue, 'yyyy-MM')		as year_month,
		'Q' + datename(qq, datevalue)		as quarter_name,
		datepart(qq, datevalue)				as quarter_number,
		format(datevalue, 'yyyy-Q') + datename(qq, datevalue) as year_quarter_name,
		format(datevalue, 'yyyy-') + datename(qq, datevalue) as year_quarter_number,
		datepart(yyyy, datevalue)			as year_value,
		case when (datepart(weekday, datevalue) + @@DATEFIRST - 2) % 7 + 1 
			BETWEEN 1 AND 5 then 'weekday'
			else 'weekend' end						as weekday_flag
	from dates
	OPTION (MAXRECURSION 20000); 

	set @rows_inserted += @@ROWCOUNT;

	--Update etl log
	insert into davidf_int.etl_run_log
		(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
	values
		('DimDate - 1st run', @start_time, SYSUTCDATETIME(), @rows_inserted, 0, 0, 'SUCCESS', null);
END;
GO

-- Stage 1.4 - etl_load_tracker
-- REQUIREMENT VG - separate table to keep track of loads
if not exists (select 1 from sys.tables where name = 'etl_load_tracker' and schema_id = SCHEMA_ID('davidf_int'))
begin
	create table davidf_int.etl_load_tracker(
		table_name				varchar(50) not null PRIMARY KEY,
		last_successful_load	datetime2 not null,
		last_successful_execution_time datetime2 null);

	--Default values to accomodate 1st run
	insert into davidf_int.etl_load_tracker
	values
		('DimCustomer', cast('1900-01-01' as datetime2), null),
		('DimSalesPerson', cast('1900-01-01' as datetime2), null),
		('DimProduct', cast('1900-01-01' as datetime2), null),
		('FactSales', cast('1900-01-01' as datetime2), null)
end
GO

-- Stage 1.5.1 - DimCustomer view
-- Create View on source data (staging/ingestion style)
-- Delta style, shows changes since last run (what to import)
create or alter view davidf_staging.src_customer as
	with et as (
		select last_successful_load as ts from davidf_int.etl_load_tracker where table_name = 'DimCustomer'
	)
	select 
		c.CustomerId									as customer_id,
		ISNULL(c.CustomerName,'') COLLATE DATABASE_DEFAULT as customer_name,
		c.PrimaryContactPersonID						as customer_main_sellerid,	-- idea to use later (BI report best sale ppl vs customers)
		ISNULL(cc.CustomerCategoryName,'') COLLATE DATABASE_DEFAULT as customer_category_name,
		c.ValidFrom										as customer_valid_from,
		c.ValidTo										as customer_valid_to,
		1												as customer_is_current
	from
		WideWorldImporters.sales.Customers c
		cross join et
		left join WideWorldImporters.sales.CustomerCategories cc
			on c.CustomerCategoryID = cc.CustomerCategoryID
	where
		c.ValidFrom > et.ts OR
		(c.ValidTo > et.ts AND c.ValidTo < cast('9999-12-31' as datetime2))

	union all

	select 
		c.CustomerId									as customer_id,
		ISNULL(c.CustomerName,'') COLLATE DATABASE_DEFAULT as customer_name,
		c.PrimaryContactPersonID						as customer_main_sellerid,	-- idea to use later (BI report best sale ppl vs customers)
		ISNULL(cc.CustomerCategoryName,'') COLLATE DATABASE_DEFAULT as customer_category_name,
		c.ValidFrom										as customer_valid_from,
		c.ValidTo										as customer_valid_to,
		0												as customer_is_current
	from
		WideWorldImporters.sales.Customers_Archive c
		cross join et
		left join WideWorldImporters.sales.CustomerCategories_Archive cc
			on c.CustomerCategoryID = cc.CustomerCategoryID
	where
		c.ValidFrom > et.ts OR
		(c.ValidTo > et.ts AND c.ValidTo < cast('9999-12-31' as datetime2))

GO

-- Stage 1.5.2 - current view (snapshot)
-- Current style, shows all currently active rows
-- (This is for update / delete logic)
create or alter view davidf_staging.src_customer_current as
	select c.CustomerID as customer_id
	from WideWorldImporters.Sales.Customers c
	where c.ValidTo > cast('9999-12-30' as datetime2)
GO

-- Stage 1.5.3 - Create DimCustomer
if not exists (select 1 from sys.tables where name = 'DimCustomer' and schema_id = SCHEMA_ID('davidf_int'))
begin
	create table davidf_int.DimCustomer(
		customer_id							int IDENTITY PRIMARY KEY,
		customer_nk							int not null,				-- map to customer_id (src)
		customer_name						varchar(100),
		customer_main_sellerid				int not null,
		customer_category_name				varchar(100),
		is_current							bit default(1) not null,
		active_from							datetime2 not null,
		active_to							datetime2 not null
	);

	-- Add unique index to verify only one id is current at any one time
	create unique index uix_dimcustomer_current	on 
		davidf_int.dimcustomer(customer_nk) where is_current = 1;
	-- Add unique index to also verify max one historical id at any time
	create unique index uix_dimcustomer_nkhistory on
		davidf_int.dimcustomer(customer_nk, active_from, active_to);
end
GO

-- Stage 1.6.1 - DimSalesPerson view
-- Create View on source data (staging/ingestion style)
-- Delta style, shows changes since last run (what to import)
create or alter view davidf_staging.src_salesperson as
	with et as (
		select last_successful_load as ts from davidf_int.etl_load_tracker where table_name = 'DimSalesPerson'
	)
	select
		p.PersonID										as person_id,
		ISNULL(p.FullName, '') collate DATABASE_DEFAULT	as person_fullname,
		right(trim(p.FullName collate DATABASE_DEFAULT), 
			charindex(' ', reverse(trim(p.fullname collate DATABASE_DEFAULT)))) as person_lastname,
		p.ValidFrom										as person_valid_from,
		p.ValidTo										as person_valid_to,
		1												as person_is_current
	from
		WideWorldImporters.Application.People p
		cross join et
	where
		(p.ValidFrom > et.ts OR
		(p.ValidTo > et.ts AND p.ValidTo < cast('9999-12-31' as datetime2))) AND
		-- Only import sales people (checked, all orders point to one of these)
		p.IsSalesperson = 1

	union all

	select
		p.PersonID										as person_id,
		ISNULL(p.FullName, '') collate DATABASE_DEFAULT	as person_fullname,
		right(trim(p.FullName collate DATABASE_DEFAULT), 
			charindex(' ', reverse(trim(p.fullname collate DATABASE_DEFAULT)))) as person_lastname,
		p.ValidFrom										as person_valid_from,
		p.ValidTo										as person_valid_to,
		0												as person_is_current
	from
		WideWorldImporters.Application.People_Archive p
		cross join et
	where
		(p.ValidFrom > et.ts OR
		(p.ValidTo > et.ts AND p.ValidTo < cast('9999-12-31' as datetime2))) AND
		-- Only import sales people (checked, all orders point to one of these)
		p.IsSalesperson = 1
GO

-- Stage 1.6.2 - current view (snapshot)
-- Snapshot style, show all currently active rows
-- (This is for update / delete logic)
create or alter view davidf_staging.src_salesperson_current as
	select p.PersonID as salesperson_id
	from WideWorldImporters.Application.People p
	where p.ValidTo > cast('9999-12-30' as datetime2) and
		p.IsSalesperson = 1
GO

-- Stage 1.6.3 - Create DimSalesPerson table
if not exists (select 1 from sys.tables where name = 'DimSalesPerson' and schema_id = SCHEMA_ID('davidf_int'))
begin
	create table davidf_int.DimSalesPerson (
		salesperson_id			int IDENTITY PRIMARY KEY,
		salesperson_nk			int not null,		-- map to person_id
		salesperson_fullname	varchar(200),
		salesperson_lastname	varchar(100),
		is_current				bit default(1) not null,
		active_from				datetime2 not null,
		active_to				datetime2 not null
	)

	-- Add unique index to verify only one id is current at any one time
	create unique index uix_dimsalesperson_current	on 
		davidf_int.dimsalesperson(salesperson_nk) where is_current = 1;
	-- Add unique index to also max one historical id at any time
	create unique index uix_dimsalesperson_nkhistory on
		davidf_int.dimsalesperson(salesperson_nk, active_from, active_to);
end
GO

-- STAGE 1.7 - DimProduct

-- Stage 1.7.1 - DimProduct view
-- Create View on source data (staging/ingestion style)
-- Delta style, shows changes since last run (what to import)
create or alter view davidf_staging.src_product as
	with et as (
		select last_successful_load as ts from davidf_int.etl_load_tracker where table_name = 'DimProduct'
	)
	select
		p.StockItemID	as product_id,
		ISNULL(p.StockItemName, '') collate DATABASE_DEFAULT as product_name,
		p.ValidFrom as valid_from,
		p.ValidTo as valid_to,
		1 as is_current
	from 
		WideWorldImporters.Warehouse.StockItems p
		cross join et
	where
		p.ValidFrom > et.ts OR
		(p.ValidTo > et.ts AND p.ValidTo < cast('9999-12-31' as datetime2))

	union all

	select
		p.StockItemID	as product_id,
		ISNULL(p.StockItemName, '') collate DATABASE_DEFAULT as product_name,
		p.ValidFrom as valid_from,
		p.ValidTo as valid_to,
		0 as is_current
	from 
		WideWorldImporters.Warehouse.StockItems_Archive p
		cross join et
	where
		p.ValidFrom > et.ts OR
		(p.ValidTo > et.ts AND p.ValidTo < cast('9999-12-31' as datetime2))
GO

-- Stage 1.7.2 - current view (snapshot)
-- Snapshot style, show all currently valid rows
-- (This is for update / delete logic)
create or alter view davidf_staging.src_products_current as
	select p.StockItemID as product_id
	from WideWorldImporters.Warehouse.StockItems p
	where p.ValidTo > cast('9999-12-30' as datetime2)
GO

-- Stage 1.7.3 - Create DimProduct
if not exists (select 1 from sys.tables where name = 'DimProduct' and schema_id = SCHEMA_ID('davidf_int'))
begin
	create table davidf_int.DimProduct (
		product_id				int IDENTITY PRIMARY KEY,
		product_skunumber_nk	int not null,		-- map to product_id
		product_name			varchar(255),
		is_current				bit default(1) not null,
		active_from				datetime2 not null,
		active_to				datetime2 not null
	)

	-- Add unique index to verify only one id is current at any one time
	create unique index uix_dimproduct_current	on 
		davidf_int.dimproduct(product_skunumber_nk) where is_current = 1;
	-- Add unique index to also max one historical id at any time
	create unique index uix_dimproduct_nkhistory on
		davidf_int.dimproduct(product_skunumber_nk, active_from, active_to);

end
GO

-- Stage 1.8.1 - FactSales view
-- Create View on source data (staging/ingestion style)
-- -----------------------------------------------------------------------------
-- INCREMENTAL LOAD STRUCTURE: Only show records with lasteditedwhen AFTER last etl_load
-- REQUIREMENTS VG 
-- NOTE: Does not allow late arriving data
-- -----------------------------------------------------------------------------
create or alter view davidf_staging.src_sales as (
	select
		cast(ol.orderid as varchar) + '-' + cast(ol.orderlineid as varchar) as order_nk,
		o.CustomerID			as order_customerid,
		o.SalespersonPersonID	as order_salespersonid,
		ol.StockItemID			as orderitem_productid,
		ISNULL(o.OrderDate, CAST('1900-01-01' as date)) as order_date,
		ol.Quantity				as orderitem_quantity,
		ol.UnitPrice			as orderitem_unitprice,
		ol.TaxRate				as orderitem_taxrate,
		(ol.Quantity * ol.UnitPrice) as orderitem_value_pretax,
		(ol.Quantity * ol.UnitPrice) * (1 - ol.TaxRate / 100) as orderitem_value_posttax,
		case when ol.LastEditedWhen > o.LastEditedWhen then
			ol.LastEditedWhen else o.LastEditedWhen end order_lasteditwhen
	from	
		WideWorldImporters.sales.orderlines ol
		join WideWorldImporters.sales.orders o on o.OrderID = ol.OrderID
	where
		case when ol.LastEditedWhen > o.LastEditedWhen then
			ol.LastEditedWhen else o.LastEditedWhen end
			> (select last_successful_load from davidf_int.etl_load_tracker where table_name = 'FactSales')
);
GO

-- Stage 1.8.2 Create FactSales
if not exists (select 1 from sys.tables where name = 'FactSales' and SCHEMA_ID = SCHEMA_ID('davidf_int'))
begin
	create table davidf_int.FactSales (
		sales_id			int IDENTITY PRIMARY KEY,
		sales_nk			varchar(50) not null,
		sales_customerid	int not null,
		sales_salespersonid	int not null,
		sales_productid		int not null,
		sales_order_datekey	int not null,
		sales_quantity		decimal(10,2),
		sales_unitprice		decimal(10,4),
		sales_taxrate		decimal(10,4),
		sales_value			decimal(10,4),
		sales_value_aftertax decimal(10,4),
		dw_last_timestamp	datetime2 not null default(SYSUTCDATETIME()),
		sales_lasteditedwhen datetime2 not null default(SYSUTCDATETIME())
	);

	-- Create FKs
	-- REQUIREMENT 1
	-- NOTE For this requirement we will create and maintain hard FKs
	-- Many datawarehouses do not want this (as far as I can tell) but prefer soft refs
	-- I have the checks for soft refs in STAGE 5, but will maintain FKs for this code
	alter table davidf_int.FactSales
		add constraint fk_factsales_dimcustomer
		foreign key (sales_customerid)
		references davidf_int.DimCustomer(customer_id);
	alter table davidf_int.FactSales
		add constraint fk_factsales_dimsalesperson
		foreign key (sales_salespersonid)
		references davidf_int.DimSalesPerson(salesperson_id);
	alter table davidf_int.FactSales
		add constraint fk_factsales_dimproduct
		foreign key (sales_productid)
		references davidf_int.DimProduct(product_id);
	alter table davidf_int.FactSales
		add constraint fk_factsales_dimdate
		foreign key (sales_order_datekey)
		references davidf_int.DimDate(date_key);

	-- Create indexes on all join keys
	-- NOTE On a small db like this these might be left alone during load
	-- On a large db with a big load these should be dropped/paused during load
	-- I will diasble during load of FactSales as "best-practise" example
	create index ix_factsales_customer on davidf_int.FactSales(sales_customerid);
	create index ix_factsales_salesperson on davidf_int.FactSales(sales_salespersonid);
	create index ix_factsales_product on davidf_int.FactSales(sales_productid);
	create index ix_factsales_orderdate on davidf_int.FactSales(sales_order_datekey);

	-- Create unique constraint
	-- NOTE: Again a hard constraint to protect integrity (as per REQUIREMENTS)
	-- I do have a test for this in STAGE 5 for a soft solution
	create unique index uix_factsales_salesnk on davidf_int.FactSales(sales_nk);
end;
GO


-- Stage 1.9 - etl_run_log
-- Log table for schedueled runs, updated in sps
-- NOTE: Due to the static data, the schedueled runs will not see updated data unless forced
-- so there will 0 in the rows. This is to be expected.
if not exists (select 1 from sys.tables where name = 'etl_run_log' and schema_id = SCHEMA_ID('davidf_int'))
	create table davidf_int.etl_run_log (
		run_id			int IDENTITY PRIMARY KEY,
		run_name		varchar(50),				-- Such as 'DimCustomer' or 'FactSales'
		start_time		datetime2,
		end_time		datetime2,
		rows_inserted	int,
		rows_updated	int,
		rows_deleted	int,
		run_status		varchar(50),				-- success/fail info
		run_error_message varchar(4000)
	);
GO



-- -----------------------------------
-- STAGE 2 / REQUIREMENT 2 
-- One-time run to load historical data 
-- ___________________________________
-- IMPORTANT: I import from BOTH current and historical source tables
-- This is then mirrored in scheduled runs so no changes are missed
-- However, only one active row per (dimension/other) should exist.
-- (Tests in Stage 5)
-- NOTE: All dimensions (not date) add a row for an 'Unknown' value.
-- This is mapped to if no "real" mapping can be done.
-- -----------------------------------

-- Stage 2.1 - DimCustomer
if (select count(*) from davidf_int.DimCustomer) = 0
begin
	declare @start_time datetime2 = SYSUTCDATETIME(),
		@rows_inserted int = 0;

	insert into davidf_int.DimCustomer
		(customer_nk, customer_name, customer_main_sellerid, customer_category_name, is_current, active_from, active_to)
		select 
			customer_id,
			customer_name,
			ISNULL(customer_main_sellerid, -1),
			customer_category_name,
			customer_is_current,
			customer_valid_from,
			customer_valid_to
		from 
			davidf_staging.src_customer

		union all

		select 
			-1,
			'Unknown',
			-1,
			'Unknown',
			1,
			cast('1900-01-01' as datetime2),
			cast('9999-12-31' as datetime2)

	set @rows_inserted += @@ROWCOUNT;

	-- Update load tracker
	-- Always set last run time
	-- This tracks when the code ran successfully
	update davidf_int.etl_load_tracker
	set last_successful_execution_time = SYSUTCDATETIME()
	where table_name = 'DimCustomer';
		
	--Update last data updated if any
	-- This tracks the latest data imported and is used for checking against new data to import
	declare @lastedit datetime2;
	select @lastedit = MAX(customer_valid_from) from davidf_staging.src_customer;
	if @lastedit is not null
		update
			davidf_int.etl_load_tracker
		set 
			last_successful_load = @lastedit
		where 
			table_name = 'DimCustomer';

	-- Loag action
	insert into davidf_int.etl_run_log
		(run_name, start_time, end_time,rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
	values
		('DimCustomer - 1st run', @start_time, SYSUTCDATETIME(), @rows_inserted, 0, 0, 'SUCCESS', null);
end			
GO

-- STAGE 2.2 - DimDate
-- Step 1 - Sanity check dates
	-- select MIN(OrderDate), MAX(OrderDate) from WideWorldImporters.Sales.Orders -- 2013-2016
	-- select MIN(OrderDate), MAX(OrderDate) from [WideWorldImporters].Purchasing.[PurchaseOrders] -- 2013-2016
	-- select MIN(ValidFrom), MAX(ValidFrom) from WideWorldImporters.Application.People -- 2013-2016
-- Decision: Create DimDate from 2000-2050
-- Step 2 - Populate DimDate according to decision
if not exists (select 1 from sys.tables where name = 'DimDate' and schema_id = SCHEMA_ID('davidf_int'))
begin
	-- Create DimDate if it does not exists
	EXEC dbo.createDimDate '2000-01-01', '2050-12-31', 1;
end
else
begin
	-- Recreate DimDate if date range does not match expectations
	--		a bit forced, but the idea is to show that even though DimDate is not supposed
	--		to be rebuilt every run it can still have some logic 
	declare @minDate date, @maxDate date;
	select @minDate = MIN(date_value), @maxDate = MAX(date_value) from davidf_int.DimDate
	if @minDate IS NULL OR @minDate > '2000-01-01' OR @maxDate < '2050-12-31'
		EXEC dbo.createDimDate '2000-01-01', '2050-12-31', 1;
end;
GO

--	Stage 2.3 - DimSalesPerson
if (select count(*) from davidf_int.DimSalesPerson) = 0
begin

	declare @start_time datetime2 = SYSUTCDATETIME(), @rows_inserted int = 0;

	insert into davidf_int.DimSalesPerson
		(salesperson_nk, salesperson_fullname, salesperson_lastname, is_current, active_from, active_to)
	select
		person_id,
		person_fullname,
		person_lastname,
		person_is_current,
		person_valid_from,
		person_valid_to
	from
		davidf_staging.src_salesperson

	union all

	select
		-1,
		'Unknown',
		'Unknown',
		1,
		cast('1900-01-01' as datetime2),
		cast('9999-12-31' as datetime2)

	set @rows_inserted += @@ROWCOUNT;

	-- Update load tracker
	-- Always set last run time
	-- This tracks when the code ran successfully
	update davidf_int.etl_load_tracker
	set last_successful_execution_time = SYSUTCDATETIME()
	where table_name = 'DimSalesPerson';
		
	--Update last data updated if any
	-- This tracks the latest data imported and is used for checking against new data to import
	declare @lastedit datetime2;
	select @lastedit = MAX(person_valid_from) from davidf_staging.src_salesperson;
	if @lastedit is not null
		update
			davidf_int.etl_load_tracker
		set 
			last_successful_load = @lastedit
		where 
			table_name = 'DimSalesPerson';

	--Update log
	insert into davidf_int.etl_run_log
		(run_name, start_time, end_time,rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
	values
		('DimSalesPerson - 1st run', @start_time, SYSUTCDATETIME(), @rows_inserted, 0, 0, 'SUCCESS', null);
end
GO

--	Stage 2.4 - DimProduct
if (select count(*) from davidf_int.DimProduct) = 0
begin

	declare @start_time datetime2 = SYSUTCDATETIME(), @rows_inserted int = 0;

	insert into davidf_int.DimProduct
		(product_skunumber_nk, product_name, is_current, active_from, active_to)
	select
		product_id,
		product_name,
		is_current,
		valid_from,
		valid_to
	from
		davidf_staging.src_product src
	where NOT EXISTS (
		select 1
		from davidf_int.DimProduct d
		where d.product_skunumber_nk = src.product_id
		  AND d.active_from          = src.valid_from
		  AND d.active_to            = src.valid_to
	)

	union all

	select
		-1,
		'Unknown',
		1,
		cast('1900-01-01' as datetime2),
		cast('9999-12-31' as datetime2)

	set @rows_inserted += @@ROWCOUNT;

	-- Update load tracker
	-- Always set last run time
	-- This tracks when the code ran successfully
	update davidf_int.etl_load_tracker
	set last_successful_execution_time = SYSUTCDATETIME()
	where table_name = 'DimProduct';
		
	--Update last data updated if any
	-- This tracks the latest data imported and is used for checking against new data to import
	declare @lastedit datetime2;
	select @lastedit = MAX(valid_from) from davidf_staging.src_product;
	if @lastedit is not null
		update
			davidf_int.etl_load_tracker
		set 
			last_successful_load = @lastedit
		where 
			table_name = 'DimProduct';

	-- Update log
	insert into davidf_int.etl_run_log
		(run_name, start_time, end_time,rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
	values
		('DimProduct - 1st run', @start_time, SYSUTCDATETIME(), @rows_inserted, 0, 0, 'SUCCESS', null);
end
GO

-- Stage 2.5 - Inital load of FactSales, run once
if (select count(*) from davidf_int.FactSales) = 0
begin
	-- Disable indexes
	alter index ix_factsales_customer on davidf_int.FactSales disable;
	alter index ix_factsales_salesperson on davidf_int.FactSales disable;
	alter index ix_factsales_product on davidf_int.FactSales disable;
	alter index ix_factsales_orderdate on davidf_int.FactSales disable;
	alter index uix_factsales_salesnk on davidf_int.FactSales disable;

	-- tracking vars
	declare @start_time datetime2 = SYSUTCDATETIME(), @rows_inserted int = 0;

	insert into
		davidf_int.FactSales(
			sales_nk,
			sales_customerid,
			sales_salespersonid,
			sales_productid,
			sales_order_datekey,
			sales_quantity,
			sales_unitprice,
			sales_taxrate,
			sales_value,
			sales_value_aftertax,
			dw_last_timestamp,
			sales_lasteditedwhen
		)
	select
		s.order_nk,
		ISNULL(c.customer_id, unknowns.unknown_customer),
		ISNULL(sp.salesperson_id, unknowns.unknown_salesperson),
		ISNULL(p.product_id, unknowns.unknown_product),
		d.date_key,
		s.orderitem_quantity,
		s.orderitem_unitprice,
		s.orderitem_taxrate,
		s.orderitem_value_pretax,
		s.orderitem_value_posttax,
		SYSUTCDATETIME(),
		s.order_lasteditwhen
	from 
		davidf_staging.src_sales s
		left join davidf_int.DimCustomer c
			on s.order_customerid = c.customer_nk
			and s.order_date BETWEEN c.active_from and c.active_to
		left join davidf_int.DimSalesPerson sp
			on s.order_salespersonid = sp.salesperson_nk
			and s.order_date BETWEEN sp.active_from and sp.active_to
		left join davidf_int.DimProduct p
			on s.orderitem_productid = p.product_skunumber_nk
			and s.order_date BETWEEN p.active_from and p.active_to
		join davidf_int.DimDate d
			on s.order_date = d.date_value
		-- ---------------------------
		-- Get sk for -1 nk (aka unknown values that cannot be matched to dims)
		-- ---------------------------
		cross join (
			select
				(select customer_id from davidf_int.DimCustomer where customer_nk = -1 and is_current = 1) as unknown_customer,
				(select salesperson_id from davidf_int.DimSalesPerson where salesperson_nk = -1 and is_current = 1) as unknown_salesperson,
				(select product_id from davidf_int.DimProduct where product_skunumber_nk = -1 and is_current = 1) as unknown_product
			) unknowns

		set @rows_inserted += @@ROWCOUNT;

	-- Enable indexes
	alter index ix_factsales_customer on davidf_int.FactSales rebuild;
	alter index ix_factsales_salesperson on davidf_int.FactSales rebuild;
	alter index ix_factsales_product on davidf_int.FactSales rebuild;
	alter index ix_factsales_orderdate on davidf_int.FactSales rebuild;
	alter index uix_factsales_salesnk on davidf_int.FactSales rebuild;

	-- Update load tracker
	-- Always set last run time
	-- This tracks when the code ran successfully
	update davidf_int.etl_load_tracker
	set last_successful_execution_time = SYSUTCDATETIME()
	where table_name = 'FactSales';
		
	--Update last data updated if any
	-- This tracks the latest data imported and is used for checking against new data to import
	declare @lastedit datetime2;
	select @lastedit = MAX(order_lasteditwhen) from davidf_staging.src_sales;
	if @lastedit is not null
		update
			davidf_int.etl_load_tracker
		set 
			last_successful_load = @lastedit
		where 
			table_name = 'FactSales';

	--log entries
	insert into davidf_int.etl_run_log
		(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
	values
		('FactSales - 1st run', @start_time, SYSUTCDATETIME(), @rows_inserted, 0, 0, 'SUCCESS', null);
end;
GO



-- -----------------------------------
-- STAGE 3 / REQUIREMENT 3 
-- (Schedueled) runs to incrementally load data
-- -----------------------------------

-- Stage 3.1.1 Create sp DimCustomer 
-- The procedure to be run on a schedule (such as nightly)
-- NOTE: This is the main procedure to update DimCustomer 
-- It will check source data (both current & history) and import any new data
create or alter procedure etl_load_dimcustomer
as
begin
	set nocount on;

	-- logging vars
	declare @start_time datetime = SYSUTCDATETIME(),
		@rows_updated int = 0, @rows_inserted int = 0, @rows_deleted int = 0;

	begin try
		begin tran;
		-- "delete" old rows that has been changed
		update
			dest
		set
			dest.is_current = 0,
			dest.active_to = src.customer_valid_from
		from 
			davidf_int.DimCustomer dest
			join davidf_staging.src_customer src
				on dest.customer_nk = src.customer_id
				and dest.is_current = 1
		where
			ISNULL(src.customer_name, '') <> ISNULL(dest.customer_name, '') OR
			ISNULL(src.customer_category_name, '') <> ISNULL(dest.customer_category_name, '') OR
			ISNULL(src.customer_main_sellerid, '') <> ISNULL(dest.customer_main_sellerid, '')

		set @rows_deleted += @@ROWCOUNT;

		-- insert new or updated rows
		insert into
			davidf_int.DimCustomer
				(customer_nk, customer_name, customer_main_sellerid, customer_category_name, is_current, active_from, active_to)
			select
				src.customer_id,
				src.customer_name,
				ISNULL(src.customer_main_sellerid, -1),
				src.customer_category_name,
				src.customer_is_current,
				src.customer_valid_from,
				src.customer_valid_to
			from
				davidf_staging.src_customer src
				left join davidf_int.DimCustomer dest
				on src.customer_id = dest.customer_nk AND
				dest.is_current = 1
			where
				dest.customer_nk IS NULL
				AND NOT EXISTS (
					select 1
					from davidf_int.DimCustomer c
					where c.customer_nk = src.customer_id
					  AND c.active_from          = src.customer_valid_from
					  AND c.active_to            = src.customer_valid_to
				)

		set @rows_inserted += @@ROWCOUNT;

		-- delete rows deleted from source
		update
			dest
		set 
			dest.is_current = 0,
			dest.active_to = SYSUTCDATETIME()
		from 
			davidf_int.DimCustomer dest
			left join davidf_staging.src_customer_current src
			on src.customer_id = dest.customer_nk
		where
			src.customer_id IS NULL AND
			dest.is_current = 1 AND
			dest.customer_nk <> -1

		set @rows_deleted += @@ROWCOUNT;

		commit tran;

		-- Update load tracker
		-- Always set last run time
		-- This tracks when the code ran successfully
		update davidf_int.etl_load_tracker
		set last_successful_execution_time = SYSUTCDATETIME()
		where table_name = 'DimCustomer';
		
		--Update last data updated if any
		-- This tracks the latest data imported and is used for checking against new data to import
		declare @lastedit datetime2;
		select @lastedit = MAX(customer_valid_from) from davidf_staging.src_customer;
		if @lastedit is not null
			update
				davidf_int.etl_load_tracker
			set 
				last_successful_load = @lastedit
			where 
				table_name = 'DimCustomer';

		-- update log
		insert into davidf_int.etl_run_log
			(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
		values (
			'DimCustomer',
			@start_time,
			SYSUTCDATETIME(),
			@rows_inserted,
			@rows_updated,
			@rows_deleted,
			'SUCCESS',
			null
		)

	end try
	begin catch
		rollback tran;
		
		-- update log
		insert into davidf_int.etl_run_log
			(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
		values (
			'DimCustomer',
			@start_time,
			SYSUTCDATETIME(),
			@rows_inserted,
			@rows_updated,
			@rows_deleted,
			'FAIL',
			ERROR_MESSAGE()
		);

		throw;
	end catch
end
GO

-- Stage 3.1.2 Run sp DimCustomer 
-- Should be run on a schedule (job)
EXEC dbo.etl_load_dimcustomer;
GO


-- Stage 3.2.1 Create sp DimSalesPerson 
-- The procedure to be run on a schedule (such as nightly)
-- NOTE: This is the main procedure to update DimSalesPerson 
-- It will check source data (both current & history) and import any new data
create or alter procedure etl_load_dimsalesperson
as
begin
	set nocount on;
	begin try

		-- logging vars
		declare @start_time datetime = SYSUTCDATETIME(),
			@rows_updated int = 0, @rows_inserted int = 0, @rows_deleted int = 0;

		begin tran;

		-- "delete" old rows that has been changed
		update
			dest
		set
			dest.is_current = 0,
			dest.active_to = src.person_valid_from
		from 
			davidf_int.DimSalesPerson dest
			join davidf_staging.src_salesperson src
				on dest.salesperson_nk = src.person_id
				and dest.is_current = 1
		where
			ISNULL(src.person_fullname, '') <> ISNULL(dest.salesperson_fullname, '')

		set @rows_deleted += @@ROWCOUNT;

		-- insert new or updated rows
		insert into
			davidf_int.DimSalesPerson
				(salesperson_nk, salesperson_fullname, salesperson_lastname, is_current, active_from, active_to)
			select
				src.person_id,
				src.person_fullname,
				src.person_lastname,
				src.person_is_current,
				src.person_valid_from,
				src.person_valid_to
			from
				davidf_staging.src_salesperson src
				left join davidf_int.DimSalesPerson dest
				on src.person_id = dest.salesperson_nk AND
				dest.is_current = 1
			where
				dest.salesperson_nk IS NULL
				AND NOT EXISTS (
					select 1
					from davidf_int.DimSalesPerson sp
					where sp.salesperson_nk		 = src.person_id
					  AND sp.active_from          = src.person_valid_from
					  AND sp.active_to            = src.person_valid_to
				)

		set @rows_inserted += @@ROWCOUNT;

		-- delete actually deleted rows
		update
			dest
		set 
			dest.is_current = 0,
			dest.active_to = SYSUTCDATETIME()
		from 
			davidf_int.DimSalesPerson dest
			left join davidf_staging.src_salesperson_current src
			on src.salesperson_id = dest.salesperson_nk
		where
			src.salesperson_id IS NULL AND
			dest.is_current = 1 AND
			dest.salesperson_nk <> -1

		set @rows_deleted += @@ROWCOUNT;

		commit tran;

		-- Update load tracker
		-- Always set last run time
		-- This tracks when the code ran successfully
		update davidf_int.etl_load_tracker
		set last_successful_execution_time = SYSUTCDATETIME()
		where table_name = 'DimSalesPerson';
		
		--Update last data updated if any
		-- This tracks the latest data imported and is used for checking against new data to import
		declare @lastedit datetime2;
		select @lastedit = MAX(person_valid_from) from davidf_staging.src_salesperson;
		if @lastedit is not null
			update
				davidf_int.etl_load_tracker
			set 
				last_successful_load = @lastedit
			where 
				table_name = 'DimSalesPerson';

		-- update log
		insert into davidf_int.etl_run_log
			(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
		values (
			'DimSalesPerson',
			@start_time,
			SYSUTCDATETIME(),
			@rows_inserted,
			@rows_updated,
			@rows_deleted,
			'SUCCESS',
			null
		);

	end try
	begin catch
		rollback tran;

		-- update log
		insert into davidf_int.etl_run_log
			(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
		values (
			'DimSalesPerson',
			@start_time,
			SYSUTCDATETIME(),
			@rows_inserted,
			@rows_updated,
			@rows_deleted,
			'FAIL',
			ERROR_MESSAGE()
		);

		throw;
	end catch
end
go

-- Stage 3.2.2 Run sp DimSalesPerson
-- Should be run on a schedule (job)
EXEC dbo.etl_load_dimsalesperson;
GO


-- Stage 3.3.1 Create sp DimProduct 
-- The procedure to be run on a schedule (such as nightly)
-- NOTE: This is the main procedure to update DimProduct 
-- It will check source data (both current & history) and import any new data
create or alter procedure etl_load_dimproduct
as
begin
	set nocount on;

	-- logging vars
	declare @start_time datetime = SYSUTCDATETIME(),
		@rows_updated int = 0, @rows_inserted int = 0, @rows_deleted int = 0;

	begin try
		begin tran;

		-- "delete" old rows that has been changed
		update
			dest
		set
			dest.is_current = 0,
			dest.active_to = src.valid_from
		from 
			davidf_int.DimProduct dest
			join davidf_staging.src_product src
				on dest.product_skunumber_nk = src.product_id
				and dest.is_current = 1
		where
			ISNULL(src.product_name, '') <> ISNULL(dest.product_name, '')

		set @rows_deleted += @@ROWCOUNT;

		-- insert new or updated rows
		insert into
			davidf_int.DimProduct
				(product_skunumber_nk, product_name, is_current, active_from, active_to)
			select
				src.product_id,
				src.product_name,
				src.is_current,
				src.valid_from,
				src.valid_to
			from
				davidf_staging.src_product src
				left join davidf_int.DimProduct dest
				on src.product_id = dest.product_skunumber_nk AND
				dest.is_current = 1
			where
				dest.product_skunumber_nk IS NULL
				AND NOT EXISTS (
					select 1
					from davidf_int.DimProduct d
					where d.product_skunumber_nk = src.product_id
					  AND d.active_from          = src.valid_from
					  AND d.active_to            = src.valid_to
				)

		set @rows_inserted += @@ROWCOUNT;

		-- delete actually deleted rows
		update
			dest
		set 
			dest.is_current = 0,
			dest.active_to = SYSUTCDATETIME()
		from 
			davidf_int.DimProduct dest
			left join davidf_staging.src_products_current src
			on src.product_id = dest.product_skunumber_nk
		where
			src.product_id IS NULL AND
			dest.is_current = 1 AND
			dest.product_skunumber_nk <> -1

		set @rows_deleted += @@ROWCOUNT;

		commit tran;

		-- Update load tracker
		-- Always set last run time
		-- This tracks when the code ran successfully
		update davidf_int.etl_load_tracker
		set last_successful_execution_time = SYSUTCDATETIME()
		where table_name = 'DimProduct';
		
		--Update last data updated if any
		-- This tracks the latest data imported and is used for checking against new data to import
		declare @lastedit datetime2;
		select @lastedit = MAX(valid_from) from davidf_staging.src_product;
		if @lastedit is not null
			update
				davidf_int.etl_load_tracker
			set 
				last_successful_load = @lastedit
			where 
				table_name = 'DimProduct';

		-- update log
		insert into davidf_int.etl_run_log
			(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
		values (
			'DimProduct',
			@start_time,
			SYSUTCDATETIME(),
			@rows_inserted,
			@rows_updated,
			@rows_deleted,
			'SUCCESS',
			null
		);

	end try
	begin catch
		rollback tran;

		-- update log
		insert into davidf_int.etl_run_log
			(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
		values (
			'DimProduct',
			@start_time,
			SYSUTCDATETIME(),
			@rows_inserted,
			@rows_updated,
			@rows_deleted,
			'FAIL',
			ERROR_MESSAGE()
		);

		throw;
	end catch
end
go

-- Stage 3.3.2 Run sp DimProduct
-- Should be run on a schedule (nightly)
EXEC dbo.etl_load_dimproduct;
GO


-- Stage 3.4.1 - Create sp for FactSales
-- The procedure to be run on a schedule (such as nightly)
-- NOTE: This is the main procedure to update FactSales 
-- It will check source data and import any new data
create or alter procedure etl_load_factsales
as
begin
	set nocount on;

	-- logging vars
	declare @start_time datetime = SYSUTCDATETIME(),
		@rows_updated int = 0, @rows_inserted int = 0, @rows_deleted int = 0;

	begin try
		begin tran;

		-- Disable indexes
		alter index ix_factsales_customer on davidf_int.FactSales disable;
		alter index ix_factsales_salesperson on davidf_int.FactSales disable;
		alter index ix_factsales_product on davidf_int.FactSales disable;
		alter index ix_factsales_orderdate on davidf_int.FactSales disable;
		alter index uix_factsales_salesnk on davidf_int.FactSales disable;

		--UPDATES ?
		-- --------------------------------------------------------------
		-- COMMENTS / THOUGHTS
		-- In many systems an update is not allowed (e.g. financial systems)
		-- Instead a correction is inserted (quantity -5 or similar, cancellations)
		-- That logic should not be here though, but in source system
		-- So for this assignment I allow updates
		-- NOTE: I do check for unkown mapping to Dimensions to allow for late data
		-- --------------------------------------------------------------
		update
			dest
		set
			dest.sales_quantity = src.orderitem_quantity,
			dest.sales_unitprice = src.orderitem_unitprice,
			dest.sales_taxrate = src.orderitem_taxrate,
			dest.sales_value = src.orderitem_value_pretax,
			dest.sales_value_aftertax = src.orderitem_value_posttax,
			dest.sales_customerid = ISNULL(src.order_customerid, unknown_customer),
			dest.sales_productid = ISNULL(src.orderitem_productid, unknown_product),
			dest.sales_salespersonid = ISNULL(src.order_salespersonid, unknown_salesperson),
			dest.dw_last_timestamp = SYSUTCDATETIME(),
			dest.sales_lasteditedwhen = src.order_lasteditwhen
		from
			davidf_staging.src_sales src
			join davidf_int.FactSales dest
			on src.order_nk = dest.sales_nk
			-- ---------------------------
			-- Get sk for -1 nk (aka unknown values that cannot be matched to dims)
			-- ---------------------------
			cross join (
				select
					(select customer_id from davidf_int.DimCustomer where customer_nk = -1 and is_current = 1) as unknown_customer,
					(select salesperson_id from davidf_int.DimSalesPerson where salesperson_nk = -1 and is_current = 1) as unknown_salesperson,
					(select product_id from davidf_int.DimProduct where product_skunumber_nk = -1 and is_current = 1) as unknown_product
				) unknowns
		where
			(src.orderitem_quantity <> dest.sales_quantity OR
			src.orderitem_unitprice <> dest.sales_unitprice OR
			src.orderitem_taxrate <> dest.sales_taxrate OR
			src.orderitem_value_pretax <> dest.sales_value OR
			src.orderitem_value_posttax <> dest.sales_value_aftertax OR
			dest.sales_productid = unknown_product OR
			dest.sales_salespersonid = unknown_salesperson OR
			dest.sales_customerid = unknown_customer) AND
			src.order_lasteditwhen > dest.sales_lasteditedwhen

		set @rows_updated += @@ROWCOUNT;

		--INSERTS
		insert into
			davidf_int.FactSales(
				sales_nk,
				sales_customerid,
				sales_salespersonid,
				sales_productid,
				sales_order_datekey,
				sales_quantity,
				sales_unitprice,
				sales_taxrate,
				sales_value,
				sales_value_aftertax,
				dw_last_timestamp,
				sales_lasteditedwhen
			)
		select
			s.order_nk,
			ISNULL(c.customer_id, unknowns.unknown_customer),
			ISNULL(sp.salesperson_id, unknowns.unknown_salesperson),
			ISNULL(p.product_id, unknowns.unknown_product),
			d.date_key,
			s.orderitem_quantity,
			s.orderitem_unitprice,
			s.orderitem_taxrate,
			s.orderitem_value_pretax,
			s.orderitem_value_posttax,
			SYSUTCDATETIME(),
			s.order_lasteditwhen
		from 
			davidf_staging.src_sales s
			left join davidf_int.DimCustomer c
				on s.order_customerid = c.customer_nk
				and s.order_lasteditwhen >= c.active_from and s.order_lasteditwhen < c.active_to
			left join davidf_int.DimSalesPerson sp
				on s.order_salespersonid = sp.salesperson_nk
				and s.order_lasteditwhen >= sp.active_from and s.order_lasteditwhen < sp.active_to
			left join davidf_int.DimProduct p
				on s.orderitem_productid = p.product_skunumber_nk
				and s.order_lasteditwhen >= p.active_from and s.order_lasteditwhen < p.active_to
			join davidf_int.DimDate d
				on s.order_date = d.date_value
			left join davidf_int.FactSales fs
				on s.order_nk = fs.sales_nk
			-- ---------------------------
			-- Get sk for -1 nk (aka unknown values that cannot be matched to dims)
			-- ---------------------------
			cross join (
				select
					(select customer_id from davidf_int.DimCustomer where customer_nk = -1 and is_current = 1) as unknown_customer,
					(select salesperson_id from davidf_int.DimSalesPerson where salesperson_nk = -1 and is_current = 1) as unknown_salesperson,
					(select product_id from davidf_int.DimProduct where product_skunumber_nk = -1 and is_current = 1) as unknown_product
				) unknowns
		where	
			fs.sales_nk IS NULL

		set @rows_inserted += @@ROWCOUNT;

		-- DELETES?
		-- ---------------------------------------------
		-- COMMENTS / THOUGHTS
		-- (aslo see updates above)
		-- Deletes should (maybe) not happen on facts, rather corrections in new rows
		-- That logic should not be here, but in source system
		-- I can add an cancelled flag but should I? That works against the idea I wrote under updates
		-- Either it should be cancelled with an equal minus row or deleted and no updates
		-- But which one is a business solution, not a coding one, and both needs to be in source first
		-- So, NO deletes... (for now)
		-- ---------------------------------------------

		-- Enable indexes
		alter index ix_factsales_customer on davidf_int.FactSales rebuild;
		alter index ix_factsales_salesperson on davidf_int.FactSales rebuild;
		alter index ix_factsales_product on davidf_int.FactSales rebuild;
		alter index ix_factsales_orderdate on davidf_int.FactSales rebuild;
		alter index uix_factsales_salesnk on davidf_int.FactSales rebuild;

		-- Update load tracker
		-- Always set last run time
		-- This tracks when the code ran successfully
		update davidf_int.etl_load_tracker
		set last_successful_execution_time = SYSUTCDATETIME()
		where table_name = 'FactSales';
		
		--Update last data updated if any
		-- This tracks the latest data imported and is used for checking against new data to import
		declare @lastedit datetime2;
		select @lastedit = MAX(order_lasteditwhen) from davidf_staging.src_sales;
		if @lastedit is not null
			update
				davidf_int.etl_load_tracker
			set 
				last_successful_load = @lastedit
			where 
				table_name = 'FactSales';

		commit tran;

		-- update log
		insert into davidf_int.etl_run_log
			(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
		values (
			'FactSales',
			@start_time,
			SYSUTCDATETIME(),
			@rows_inserted,
			@rows_updated,
			@rows_deleted,
			'SUCCESS',
			null
		);

	end try
	begin catch
		rollback tran;

		-- update log
		insert into davidf_int.etl_run_log
			(run_name, start_time, end_time, rows_inserted, rows_updated, rows_deleted, run_status, run_error_message)
		values (
			'FactSales',
			@start_time,
			SYSUTCDATETIME(),
			@rows_inserted,
			@rows_updated,
			@rows_deleted,
			'FAIL',
			ERROR_MESSAGE()
		);

		throw;
	end catch
end
GO

-- Stage 3.4.2 - Run sp for FactSales
-- Should be run on a schedule (nigthly)
EXEC dbo.etl_load_factsales;
GO



-- -----------------------------------
-- STAGE 4 / OUTSIDE REQUIREMENTS
-- Optional simple views for BI/apps
-- -----------------------------------

-- Stage 4.1 - DimCustomer view for analytics
-- DESIGN CHOICE: Removes non-active rows (a simple "view" for BI)
create or alter view davidf_mart.CustomerCurrent as (
	select 
		customer_id,
		customer_name,
		customer_main_sellerid, 
		customer_category_name
	from
		davidf_int.DimCustomer
	where
		is_current = 1
);
GO

-- Stage 4.2 - DimSalesPerson view for analytics
-- DESIGN CHOICE: Removes non-active rows (a simple "view" for BI)
create or alter view davidf_mart.SalesPersonCurrent as (
	select 
		salesperson_id,
		salesperson_fullname,
		salesperson_lastname 
	from
		davidf_int.DimSalesPerson
	where
		is_current = 1
);
GO

-- Stage 4.3 - DimProduct view for analytics
-- DESIGN CHOICE: Removes non-active rows (a simple "view" for BI)
create or alter view davidf_mart.ProductCurrent as (
	select 
		product_id,
		product_name 
	from
		davidf_int.DimProduct
	where
		is_current = 1
);
GO

-- Stage 4.4 - FactSales view for analytics
-- DESIGN CHOICE: Add this so BI/analytics can be restricted to marts schema or at least find it easily
create or alter view davidf_mart.FactSales as (
	select
		sales_id,
		sales_customerid,
		sales_salespersonid,
		sales_productid,
		sales_order_datekey,
		sales_quantity,
		sales_unitprice,
		sales_taxrate,
		sales_value,
		sales_value_aftertax
	from
		davidf_int.FactSales
);
GO



-- --------------------------------------------
-- STAGE 5 Testing and validations
-- All normal tests should return 0 / nothing
-- DimDate tests for swedish language (REQUIREMENT)
-- Ref tests (KINDA/MAYBE/NOT REQUIREMENT)
-- Load tracker dates is a manual test (shows all in both tracker and log)
-- NOTE: In production these should be in the pipeline and stop each stage on error
-- --------------------------------------------


-- Stage 5.1 Check full table structure
select
	table_name as missing_table
from (values
    ('DimDate'),
    ('DimCustomer'),
    ('DimSalesPerson'),
    ('DimProduct'),
    ('FactSales'),
    ('etl_load_tracker')
) int_tables(table_name)
where
    not exists (select 1 from sys.tables where name = table_name and schema_id = SCHEMA_ID('davidf_int'));

-- Stage 5.2 Check DimDate
-- Unique dates
select
    date_value,
    count(*) as cnt
from
    davidf_int.DimDate
group by
    date_value
having
    count(*) > 1;
-- Swedish days
select distinct day_of_week_name
from davidf_int.DimDate;
--Swedish months
select distinct month_name
from davidf_int.DimDate;

-- Stage 5.3 Check DimCustomer
-- Unique nk for is_current
select customer_nk, count(*) as cnt
from davidf_int.DimCustomer
where is_current = 1
group by customer_nk having count(*) > 1;
-- Missing active_from / active_to
select *
from davidf_int.DimCustomer
where active_from is null OR active_to is null;
-- Overlapping validity periods
select 
    c1.customer_nk,
    c1.customer_id as customer_id_1,
    c1.active_from as active_from_1,
    c1.active_to   as active_to_1,
    c2.customer_id as customer_id_2,
    c2.active_from as active_from_2,
    c2.active_to   as active_to_2
from davidf_int.DimCustomer c1
    join davidf_int.DimCustomer c2
    on c1.customer_nk = c2.customer_nk AND
   c1.customer_id <> c2.customer_id AND
   cast(c1.active_from as date) < cast(c2.active_to as date) AND
   cast(c2.active_from as date) < cast(c1.active_to as date)
ORDER BY c1.customer_nk;

-- Stage 5.4 Check DimSalesPerson
-- Unique nk for is_current
select salesperson_nk, count(*) as cnt
from davidf_int.DimSalesPerson
where is_current = 1
group by salesperson_nk having count(*) > 1;
-- Missing active_from / active_to
select *
from davidf_int.DimSalesPerson
where active_from is null OR active_to is null;
select 
    s1.salesperson_nk,
    s1.salesperson_id as salesperson_id_1,
    s1.active_from    as active_from_1,
    s1.active_to      as active_to_1,
    s2.salesperson_id as salesperson_id_2,
    s2.active_from    as active_from_2,
    s2.active_to      as active_to_2
from davidf_int.DimSalesPerson s1
    join davidf_int.DimSalesPerson s2
    on s1.salesperson_nk = s2.salesperson_nk AND
    s1.salesperson_id <> s2.salesperson_id AND
    cast(s1.active_from as date) < cast(s2.active_to as date) AND
    cast(s2.active_from as date) < cast(s1.active_to as date)
order by s1.salesperson_nk;


-- Stage 5.5 Check DimProduct
-- Unique nk for is_current
select product_skunumber_nk, count(*) as cnt
from davidf_int.DimProduct
where is_current = 1
group by product_skunumber_nk having count(*) > 1;
-- Missing active_from / active_to
select *
from davidf_int.DimProduct
where active_from is null OR active_to is null;
-- Overlapping validity periods
select 
    p1.product_skunumber_nk,
    p1.product_id as product_id_1,
    p1.active_from as active_from_1,
    p1.active_to as active_to_1,
    p2.product_id as product_id_2,
    p2.active_from as active_from_2,
    p2.active_to as active_to_2
from davidf_int.DimProduct p1
    join davidf_int.DimProduct p2
    on p1.product_skunumber_nk = p2.product_skunumber_nk AND
    p1.product_id <> p2.product_id AND
    cast(p1.active_from as date) < cast(p2.active_to as date) AND
    cast(p2.active_from as date) < cast(p1.active_to as date)
ORDER BY p1.product_skunumber_nk;

-- Stage 5.6 Check FactSales
-- duplicate nk
select sales_nk, count(*) as cnt
from davidf_int.FactSales
group by sales_nk having count(*) > 1;


-- ----------------------------------------------
-- CHECK ref integrity
-- I know the requirements here are to use hard FK refs
-- but dbt/databricks and other datawarehouses seem to prefer soft refs with checks
-- so I have written some checks here
-- ----------------------------------------------
-- DimCustomer ref
select top 100
    f.*
from 
    davidf_int.FactSales f
    left join davidf_int.DimCustomer d on d.customer_id = f.sales_customerid
where
    d.customer_id IS NULL;
--DimSalesPerson ref
select top 100
    f.*
from 
    davidf_int.FactSales f
    left join davidf_int.DimSalesPerson d on d.salesperson_id = f.sales_salespersonid
where
    d.salesperson_id IS NULL;
-- DimProduct ref
select top 100
    f.*
from 
    davidf_int.FactSales f
    left join davidf_int.DimProduct d on d.product_id = f.sales_productid
where
    d.product_id IS NULL;
-- DimDate ref
select top 100
    f.*
from 
    davidf_int.FactSales f
    left join davidf_int.DimDate d on d.date_key = f.sales_order_datekey
where
    d.date_key IS NULL;

-- Stage 5.7 Check load tracker
-- check that all loads have dates
-- NOTE: This is mostly for this assignment
select *
from davidf_int.etl_load_tracker;

--Stage 5.8 Check log
-- NOTE: This is mostly for this assignment
-- You can run F5 many times and watch what happens here
select *
from davidf_int.etl_run_log;



-- --------------------------------------------
-- STAGE 6 Scheduling and jobs
-- Scheduled jobs (suggested) as per below
-- Results logged in etl_run_log
-- etl_load_tracker keeps track of last run
-- NOTE: DimDate is not designed to be run unless manually 
--      EXEC dbo.etl_load_dimcustomer
--      EXEC dbo.etl_load_dimsalesperson
--      EXEC dbo.etl_load_dimproduct
--      EXEC dbo.etl_load_factsales  ** Always last **
-- Stage 5 tests shoud be run after this
--  either in a sp or standalone tests (dbt, databricks similar)
-- --------------------------------------------


-- -----------------------------------------------------------------
-- STAGE X Not in REQUIREMENTS - but important
-- These are the sanity checks I did after implementation
-- Check that total values match source and destination
-- -----------------------------------------------------------------

/*
-- Test FactSales -- all sales should be imported so sum sales values should add up
select sum(quantity*unitprice) as src_sales_total from WideWorldImporters.Sales.OrderLines
select sum(sales_value) dest_sales_total from davidf_int.FactSales
-- number of rows should also match
select count(*) as src_sales_rows from WideWorldImporters.Sales.OrderLines
select count(*) dest_sales_rows from davidf_int.FactSales

-- Test DimCustomer -- No of active customers should match (+1 DimCustomer for unknown)
select count(*) from WideWorldImporters.Sales.Customers where GETDATE() between ValidFrom and ValidTo
select count(*) from davidf_int.DimCustomer where is_current = 1

-- Test DimProduct -- No of active products should match (+1 DimProduct for Unknown)
select count(*) from WideWorldImporters.Warehouse.StockItems where GETDATE() between ValidFrom and ValidTo
select count(*) from davidf_int.DimProduct where is_current = 1

-- Test DimSalesPerson -- No of active people should match (+1 DimSalesPerson for Unknown)
select count(*) from WideWorldImporters.Application.People where getdate() between ValidFrom and ValidTo and isSalesPerson = 1
select count(*) from davidf_int.DimSalesPerson where is_current = 1
*/
