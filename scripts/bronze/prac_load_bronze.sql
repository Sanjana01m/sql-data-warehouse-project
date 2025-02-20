use DATAWAREHOUSE;
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
	BEGIN
	DECLARE @starttime DATETIME , @endtime DATETIME;
	BEGIN TRY
		PRINT'==============================================================';
		PRINT'LOADINGG BRONZEE LAYER';
		PRINT'==============================================================';
		PRINT'LOADING CRM TABLE';
		PRINT'---------------------------------------------------------------';
		SET @starttime = GETDATE();
		PRINT'TRUNCATING TABLE BRONZE.crm_cust_info';
			TRUNCATE TABLE BRONZE.crm_cust_info;

		PRINT'>>INSERTING DATA INTO :BRONZE.crm_cust_info';
			BULK INSERT BRONZE.crm_cust_info FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
				WITH(FIRSTROW = 2,
					 FIELDTERMINATOR=',',
					 TABLOCK);
		SET @endtime = GETDATE();

		SET @starttime = GETDATE();
		PRINT'TRUNCATING TABLE BRONZE.crm_prd_info';
			TRUNCATE TABLE BRONZE.crm_prd_info;

		PRINT'>>INSERTING DATA INTO :BRONZE.crm_cust_info';
			BULK INSERT BRONZE.crm_prd_info FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH(FIRSTROW =2,
				FIELDTERMINATOR =',',
				TABLOCK);
		SET @endtime = GETDATE();

		SET @starttime = GETDATE();
		PRINT'TRUNCATING TABLE BRONZE.crm_sales_details';
			TRUNCATE TABLE BRONZE.crm_sales_details;

		PRINT'>>INSERTING DATA INTO :BRONZE.crm_cust_info';
			BULK INSERT BRONZE.crm_sales_details FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH(FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK);

		SET @endtime = GETDATE();

		PRINT'LOADING ERP TABLE';
		PRINT'---------------------------------------------------------------';
		SET @starttime = GETDATE();
		PRINT'TRUNCATING TABLE BRONZE.erp_cust_az12';
			TRUNCATE TABLE BRONZE.erp_cust_az12;

		PRINT'>>INSERTING DATA INTO :BRONZE.crm_cust_info';
			BULK INSERT BRONZE.erp_cust_az12 FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			WITH(FIRSTROW=2,
			FIELDTERMINATOR =',',
			TABLOCK);

		SET @endtime = GETDATE();

		SET @starttime = GETDATE();
		PRINT'TRUNCATING TABLE BRONZE.erp_loc_a101';
			TRUNCATE TABLE BRONZE.erp_loc_a101;

		PRINT'>>INSERTING DATA INTO :BRONZE.crm_cust_info';
			BULK INSERT BRONZE.erp_loc_a101 FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			WITH (FIRSTROW =2,
			FIELDTERMINATOR=',',
			TABLOCK);

		SET @endtime = GETDATE();

		SET @starttime = GETDATE();
		PRINT'TRUNCATING TABLE BBRONZE.erp_px_cat_g1v2';
			TRUNCATE TABLE BRONZE.erp_px_cat_g1v2;

		PRINT'>>INSERTING DATA INTO :BRONZE.crm_cust_info';
			BULK INSERT BRONZE.erp_px_cat_g1v2 FROM 'E:\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			WITH(FIRSTROW =2,
			FIELDTERMINATOR =',',
			TABLOCK);

		SET @endtime = GETDATE();
	END TRY
	BEGIN CATCH
		PRINT'ERROR OCCURED';
		PRINT'ERROR MESSAGE'+ERROR_MESSAGE();
	END CATCH
END
