--PAYMENT METHODS DM

CREATE OR REPLACE PROCEDURE bl_cl.add_payment_methods_to_dm()
AS $$
DECLARE
    cur_data 	CURSOR FOR SELECT * FROM bl_3nf.ce_payment_methods
   				WHERE payment_method_id != -1;
    rec bl_3nf.ce_payment_methods%ROWTYPE;
   	v_row_count INT = 0;
   	v_message VARCHAR(100);
   	v_procedure_name VARCHAR(50);
BEGIN
	SELECT 'bl_cl.add_payment_methods_to_dm' INTO v_procedure_name;
    OPEN cur_data;
    LOOP
        FETCH NEXT FROM cur_data INTO rec;
       	EXIT WHEN NOT FOUND;
       	IF NOT EXISTS 	(SELECT 1
						FROM bl_dm.dim_payment_methods dmp
						WHERE dmp.payment_method_src_id = rec.payment_method_id::VARCHAR)
			THEN
			BEGIN
		        INSERT INTO bl_dm.dim_payment_methods 	(payment_method_surr_id,
		        										payment_method_src_id,
		        										payment_method,
		        										ta_insert_dt,
		        										ta_update_dt,
		        										source_system, source_entity)
		       	VALUES (NEXTVAL('bl_dm.seq_dim_payment_methods'),
		       			COALESCE(rec.payment_method_id::VARCHAR, 'n. a.'),
		       			COALESCE(rec.payment_method::VARCHAR, 'n. a.'),
		       			NOW()::DATE,
		       			NOW()::DATE,
		       			'bl_3nf', 'ce_payment_methods');
		       	v_row_count = v_row_count + 1;
    		END;
    	END IF;
    END LOOP;
    CLOSE cur_data;
	v_message = 'Success';
	--
  	CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	--
  	--COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
END;
$$ 
LANGUAGE plpgsql;

--PROMOTIONS DM

CREATE OR REPLACE PROCEDURE bl_cl.add_promotions_to_dm()
AS $$
DECLARE
   	v_row_count INT = 0;
   	v_message VARCHAR(100);
   	v_procedure_name VARCHAR(50);
BEGIN
	SELECT 'bl_cl.add_promotions_to_dm' INTO v_procedure_name;
	BEGIN
		MERGE INTO bl_dm.dim_promotions dm
		USING (SELECT promotion_id, promotion_name FROM bl_3nf.ce_promotions WHERE promotion_src_id <> 'n. a.') AS nf
		ON dm.promotion_src_id = nf.promotion_id::VARCHAR
		WHEN MATCHED AND dm.promotion_name <> nf.promotion_name THEN
			UPDATE SET promotion_name = nf.promotion_name
		WHEN NOT MATCHED THEN
			INSERT (promotion_surr_id,
					promotion_src_id,
					promotion_name,
					ta_insert_dt,
					ta_update_dt,
					source_system, source_entity)
			VALUES 	(NEXTVAL('bl_dm.seq_dim_promotions'),
					COALESCE(nf.promotion_id::VARCHAR, 'n. a.'),
					COALESCE(nf.promotion_name, 'n. a.'),
					NOW()::DATE, NOW()::DATE,
					'bl_3nf', 'ce_promotions');
		GET DIAGNOSTICS v_row_count = ROW_COUNT;
	END;
	v_message = 'Success';
	CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	--
	--COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
END;
$$ 
LANGUAGE plpgsql;

--PRODUCTS DM

CREATE OR REPLACE PROCEDURE bl_cl.add_products_to_dm()
AS $$
DECLARE
	v_row_count INT = 0;
   	v_message VARCHAR(100);
   	v_procedure_name VARCHAR(50);
BEGIN 
	SELECT 'bl_cl.add_products_to_dm' INTO v_procedure_name;
	MERGE INTO bl_dm.dim_products pdm
	USING (
	SELECT p.product_id, p.product_name, p.category_id, c.category_name
	FROM bl_3nf.ce_products p
	LEFT JOIN bl_3nf.ce_categories c ON p.category_id = c.category_id
	WHERE p.product_src_id != 'n. a.') AS nf
	ON pdm.product_src_id = nf.product_id::VARCHAR
	WHEN MATCHED THEN
			DO NOTHING
	WHEN NOT MATCHED THEN
		INSERT  (product_surr_id,
				product_src_id,
				product_name,
				product_category_id,
				product_category_name,
				ta_insert_dt,
				ta_update_dt,
				source_system,
				source_entity)
		VALUES  (NEXTVAL('bl_dm.seq_dim_products'),
				COALESCE(nf.product_id::VARCHAR, 'n. a.'),
				COALESCE(nf.product_name,'n. a.'),
				COALESCE(nf.category_id, '-1'),
				COALESCE(nf.category_name, 'n. a.'),
				NOW()::DATE,
				NOW()::DATE,
				'bl_3nf',
				'ce_products, ce_categories');
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
					v_message = 'Success';
	CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	--
	--COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);				
END;
$$
LANGUAGE plpgsql;

--STORES DM

CREATE OR REPLACE PROCEDURE bl_cl.add_stores_to_dm()
AS $$
DECLARE
	v_row_count INT = 0;
   	v_message VARCHAR(100);
   	v_procedure_name VARCHAR(50);
BEGIN 
	SELECT 'bl_cl.add_stores_to_dm' INTO v_procedure_name;
	MERGE INTO bl_dm.dim_stores sdm
	USING (
	SELECT s.store_id, s.store_name, s.address_id, a.address_street_address, a.city_id, c.city_name
	FROM bl_3nf.ce_stores s
	LEFT JOIN bl_3nf.ce_addresses a ON s.address_id = a.address_id
	LEFT JOIN bl_3nf.ce_cities c ON a.city_id = c.city_id
	WHERE s.store_src_id <> 'n. a.') AS nf
	ON sdm.store_src_id = nf.store_id::VARCHAR
	WHEN MATCHED THEN
			DO NOTHING
	WHEN NOT MATCHED THEN
		INSERT  (store_surr_id,
				store_src_id,
				store_name,
				store_address_id,
				store_city_id,
				store_address,
				ta_insert_dt,
				ta_update_dt,
				source_system,
				source_entity)
		VALUES  (NEXTVAL('bl_dm.seq_dim_stores'),
				COALESCE(nf.store_id::VARCHAR, 'n. a.'),
				COALESCE(nf.store_name,'n. a.'),
				COALESCE(nf.address_id, -1),
				COALESCE(nf.city_id, -1),
				COALESCE(ROW(nf.address_street_address, nf.city_name)::bl_dm.address, ROW('n. a.', 'n. a.')::bl_dm.address),
				NOW()::DATE,
				NOW()::DATE,
				'bl_3nf',
				'ce_stores, ce_addresses, ce_cities');
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
					v_message = 'Success';
	CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	--
	--COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);					
END;
$$
LANGUAGE plpgsql;

--EMPLOYEES DM

CREATE OR REPLACE PROCEDURE bl_cl.add_employees_to_dm()
AS $$
DECLARE
	v_row_count INT = 0;
   	v_message VARCHAR(100);
   	v_procedure_name VARCHAR(50);
BEGIN
	SELECT 'bl_cl.add_employees_to_dm' INTO v_procedure_name;
	MERGE INTO bl_dm.dim_employees edm
	USING (
	SELECT 	e.employee_id, 				e.employee_first_name, 		e.employee_last_name,
			e.employee_date_of_birth, 	e.employee_gender, 			e.employee_email,
			e.address_id, 				a.address_street_address, 	a.city_id, 				c.city_name
	FROM bl_3nf.ce_employees e
	LEFT JOIN bl_3nf.ce_addresses a ON e.address_id = a.address_id
	LEFT JOIN bl_3nf.ce_cities c ON a.city_id = c.city_id
	WHERE e.employee_src_id <> 'n. a.') AS nf
	ON edm.employee_src_id = nf.employee_id::VARCHAR
	WHEN MATCHED THEN
			DO NOTHING
	WHEN NOT MATCHED THEN
		INSERT  (employee_surr_id, 	employee_src_id,		employee_first_name,
				employee_last_name, employee_date_of_birth, employee_gender,
				employee_email,		employee_address_id,	employee_street_address,
				employee_city_id,	employee_city_name,		ta_insert_dt,
				ta_update_dt,		source_system,			source_entity)
		VALUES  (NEXTVAL('bl_dm.seq_dim_employees'),		COALESCE(nf.employee_id::VARCHAR, 'n. a.'),					COALESCE(nf.employee_first_name,'n. a.'),
				COALESCE(nf.employee_last_name,'n. a.'),	COALESCE(nf.employee_date_of_birth::DATE, date '1900-1-1'), COALESCE(nf.employee_gender,'n. a.'),
				COALESCE(nf.employee_email, 'n. a.'),		COALESCE(nf.address_id, -1),								COALESCE(nf.address_street_address,'n. a.'),
				COALESCE(nf.city_id, -1),					COALESCE(nf.city_name, 'n. a.'),							NOW()::DATE,
				NOW()::DATE,								'bl_3nf',													'ce_employees, ce_addresses, ce_cities');
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
					v_message = 'Success';
    CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	--
   	--COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
END;
$$
LANGUAGE plpgsql;

--CUSTOMERS DM

CREATE OR REPLACE PROCEDURE bl_cl.add_customers_to_dm()
AS $$
DECLARE
	v_row_count INT = 0;
   	v_message VARCHAR(100);
   	v_procedure_name VARCHAR(50);
BEGIN
	SELECT 'bl_cl.add_customers_to_dm' INTO v_procedure_name;
	MERGE INTO bl_dm.dim_customers_scd cdm
	USING (
	SELECT 	c.customer_id,				c.customer_first_name, 		c.customer_last_name,
			c.customer_date_of_birth,	c.customer_gender,			c.customer_email,
			c.address_id,				a.address_street_address, 	a.city_id, 	cc.city_name, c.start_dt
	FROM bl_3nf.ce_customers_scd c
	LEFT JOIN bl_3nf.ce_addresses a ON c.address_id = a.address_id
	LEFT JOIN bl_3nf.ce_cities cc ON a.city_id = cc.city_id
	WHERE c.customer_src_id <> 'n. a.') AS nf
	ON cdm.customer_src_id = nf.customer_id::VARCHAR
	WHEN MATCHED AND 	(SELECT COUNT(*)
						FROM bl_dm.dim_customers_scd cdm
						WHERE cdm.start_dt = nf.start_dt) = 0 AND 
						(cdm.customer_email <> nf.customer_email OR
						cdm.customer_first_name <> nf.customer_first_name OR
						cdm.customer_last_name <> nf.customer_last_name OR
						cdm.customer_date_of_birth <> nf.customer_date_of_birth OR
						cdm.customer_gender <> nf.customer_gender OR
						cdm.customer_address_id <> nf.address_id) THEN
		UPDATE SET is_active = FALSE
	WHEN NOT MATCHED THEN
		INSERT  (customer_surr_id,	customer_src_id,		customer_first_name,
				customer_last_name,	customer_date_of_birth,	customer_gender,
				customer_email,		customer_address_id,	customer_street_address,
				customer_city_id,	customer_city_name,		start_dt,
				end_dt,				is_active,				ta_insert_dt,
				source_system,		source_entity)
		VALUES  (NEXTVAL('bl_dm.seq_dim_customers_scd'),	COALESCE(nf.customer_id::VARCHAR, 'n. a.'),					COALESCE(nf.customer_first_name,'n. a.'),
				COALESCE(nf.customer_last_name,'n. a.'),	COALESCE(nf.customer_date_of_birth::DATE, date '1900-1-1'), COALESCE(nf.customer_gender,'n. a.'),
				COALESCE(nf.customer_email, 'n. a.'),		COALESCE(nf.address_id, -1),								COALESCE(nf.address_street_address,'n. a.'),
				COALESCE(nf.city_id, -1),					COALESCE(nf.city_name, 'n. a.'),							COALESCE(nf.start_dt, date '1900-1-1'),
				DATE '9999-12-31', 							TRUE,														NOW()::DATE,
				'bl_3nf',									'ce_customers_scd, ce_addresses, ce_cities');
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	v_message = 'Success';
	CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	--
	--COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
END;
$$
LANGUAGE plpgsql;

--FACTS DM

CREATE OR REPLACE PROCEDURE bl_cl.add_facts_to_dm()
AS $$
DECLARE
	v_row_count INT = 0;
   	v_message VARCHAR(100);
   	v_procedure_name VARCHAR(50);
BEGIN
	SELECT 'bl_cl.add_facts_to_dm' INTO v_procedure_name;
	WITH
	fact_cte AS (
	SELECT 	p.product_surr_id,
			c.customer_surr_id,
			s.store_surr_id,
			e.employee_surr_id,
			pr.promotion_surr_id,
			pm.payment_method_surr_id,
			nf.sale_transaction_id,
			d.date_id,
			nf.sale_quantity,
			nf.sale_revenue,
			nf.sale_regular_price,
			nf.sale_cost,
			nf.sale_amount,
			nf.ta_insert_dt,
			nf.ta_update_dt,
			c.is_active,
			c.customer_src_id
	FROM bl_3nf.ce_sales nf
	LEFT JOIN bl_dm.dim_products p ON nf.product_id::VARCHAR = p.product_src_id
	LEFT JOIN bl_dm.dim_customers_scd c ON nf.customer_id::VARCHAR = c.customer_src_id AND c.is_active = TRUE
	LEFT JOIN bl_dm.dim_stores s ON nf.store_id::VARCHAR = s.store_src_id
	LEFT JOIN bl_dm.dim_employees e ON nf.employee_id::VARCHAR = e.employee_src_id
	LEFT JOIN bl_dm.dim_promotions pr ON nf.promotion_id::VARCHAR = pr.promotion_src_id
	LEFT JOIN bl_dm.dim_payment_methods pm ON nf.payment_method_id::VARCHAR = pm.payment_method_src_id
	LEFT JOIN bl_dm.dim_dates d ON nf.event_dt = d.date_date
	WHERE NOT EXISTS (SELECT 1 
						FROM bl_dm.fct_sales_dd dm
						WHERE dm.sale_transaction_id = nf.sale_transaction_id))
	INSERT INTO bl_dm.fct_sales_dd (product_surr_id,	customer_surr_id,	store_surr_id,
									employee_surr_id,	promotion_surr_id,	payment_method_surr_id,
									sale_transaction_id,sale_profit_margin,	sale_return_of_investment,
									event_dt,			sale_quantity,		sale_revenue,	sale_regular_price,
									sale_cost,			sale_amount,		ta_insert_dt,	ta_update_dt)
	SELECT	cte.product_surr_id, cte.customer_surr_id, cte.store_surr_id,
			cte.employee_surr_id, cte.promotion_surr_id, cte.payment_method_surr_id,
			cte.sale_transaction_id, (cte.sale_revenue / cte.sale_cost) / NULLIF(cte.sale_revenue, 0),
			(cte.sale_amount / cte.sale_cost) * 100,
			cte.date_id, cte.sale_quantity, cte.sale_revenue, cte.sale_regular_price,
			cte.sale_cost, cte.sale_amount, NOW(), NOW()
	FROM fact_cte cte
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_dm.fct_sales_dd f 
						WHERE 	cte.customer_surr_id::INT = f.customer_surr_id AND cte.is_active IS FALSE AND
							cte.product_surr_id::INT = f.product_surr_id AND
							cte.store_surr_id::INT = f.store_surr_id AND
							cte.employee_surr_id::INT = f.employee_surr_id AND
							cte.promotion_surr_id::INT = f.promotion_surr_id AND
							cte.payment_method_surr_id::INT = f.payment_method_surr_id);
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	UPDATE sa_card_sales.src_card_sales
	SET is_processed = TRUE;
	UPDATE sa_cash_sales.src_cash_sales
	SET is_processed = TRUE;
	v_message = 'Success';
	CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	--COMMIT;
	EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);	
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE bl_cl.run_etl()
AS $$
DECLARE
	v_t TIMESTAMP;
BEGIN 
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_payment_methods_to_3nf();
	RAISE INFO 'Payment methods added to 3nf, % ', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_promotions_to_3nf();
	RAISE INFO 'Promotions added to 3nf, %', CLOCK_TIMESTAMP() - v_t;	
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_categories_to_3nf();
	RAISE INFO 'Categories added to 3nf, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_products_to_3nf();
	RAISE INFO 'Products added to 3nf, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_cities_to_3nf();
	RAISE INFO 'Cities added to 3nf, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_addresses_to_3nf();
	RAISE INFO 'Addresses added to 3nf, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_employees_to_3nf();
	RAISE INFO 'Employees added to 3nf, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_stores_to_3nf();
	RAISE INFO 'Stores added to 3nf, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_customers_to_3nf();
	RAISE INFO 'Customers added to 3nf, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_facts_to_3nf();
	RAISE INFO 'Facts added to 3nf, %', CLOCK_TIMESTAMP() - v_t;
	COMMIT;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_payment_methods_to_dm();
	RAISE INFO 'Payment methods added to bl_dm, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_promotions_to_dm();
	RAISE INFO 'Promotions added to bl_dm, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_products_to_dm();
	RAISE INFO 'Products added to bl_dm, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_stores_to_dm();
	RAISE INFO 'Stores added to bl_dm, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_employees_to_dm();
	RAISE INFO 'Employees added to bl_dm, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_customers_to_dm();
	RAISE INFO 'Customers added to bl_dm, %', CLOCK_TIMESTAMP() - v_t;
	v_t = CLOCK_TIMESTAMP();
	CALL bl_cl.add_facts_to_dm();	
	RAISE INFO 'Facts added to bl_dm, %', CLOCK_TIMESTAMP() - v_t;
	COMMIT;
END;
$$
LANGUAGE plpgsql;






