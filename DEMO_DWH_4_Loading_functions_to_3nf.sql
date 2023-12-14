--PAYMENT METHODS

CREATE OR REPLACE PROCEDURE bl_cl.add_payment_methods_to_3nf() 
AS $$
	DECLARE
		v_procedure_name VARCHAR(50);
		v_message VARCHAR(100);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;
BEGIN
	SELECT 'bl_cl.add_payment_methods_to_3nf' INTO v_procedure_name;
	WITH payment_methods_cte AS (
		SELECT DISTINCT payment_method_id,
						payment, 
						'sa_cash_sales' AS source_system,
						'src_cash_sales' AS source_entity
		FROM sa_cash_sales.src_cash_sales
		WHERE is_processed IS FALSE)
		INSERT INTO bl_3nf.ce_payment_methods (payment_method_id,
												payment_method_src_id,
												payment_method,
												ta_insert_dt,
												ta_update_dt,
												source_system,
												source_entity)
		SELECT 	NEXTVAL('bl_3nf.payment_method_id_seq'),
				payment_method_id,
				payment,
				NOW()::DATE,
				NOW()::DATE,
				source_system,
				source_entity
		FROM payment_methods_cte
		WHERE NOT EXISTS 	(SELECT 1
							FROM bl_3nf.ce_payment_methods
							WHERE payment_method_src_id = payment_methods_cte.payment_method_id AND
									source_system = payment_methods_cte.source_system);
		GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
		WITH payment_methods_cte AS (
		SELECT DISTINCT payment_method_id,
						payment_method,
						'sa_card_sales' AS source_system,
						'src_card_sales' AS source_entity
		FROM sa_card_sales.src_card_sales
		WHERE is_processed IS FALSE)
		INSERT INTO bl_3nf.ce_payment_methods (payment_method_id,
										payment_method_src_id,
										payment_method,
										ta_insert_dt,
										ta_update_dt,
										source_system,
										source_entity)
		SELECT 	NEXTVAL('bl_3nf.payment_method_id_seq'),
				payment_method_id,
				payment_method,
				NOW()::DATE,
				NOW()::DATE,
				source_system,
				source_entity
		FROM payment_methods_cte
		WHERE NOT EXISTS 	(SELECT 1
							FROM bl_3nf.ce_payment_methods
							WHERE payment_method_src_id = payment_methods_cte.payment_method_id AND
									source_system = payment_methods_cte.source_system);
		GET DIAGNOSTICS v_row_count_card = ROW_COUNT;
  						v_message = 'Success';
  						v_row_count = v_row_count_cash + v_row_count_card;
		--
  		CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
  	--COMMIT;
	--
    EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
END;
$$
LANGUAGE plpgsql;

--PROMOTIONS

CREATE OR REPLACE PROCEDURE bl_cl.add_promotions_to_3nf()
AS $$
	DECLARE
		v_procedure_name VARCHAR(50);
		v_message VARCHAR(100);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;
BEGIN
	SELECT 'bl_cl.add_promotions_to_3nf' INTO v_procedure_name;
   	WITH promotions_cte AS (
		SELECT DISTINCT promotion_id,
						promotion,
						'sa_cash_sales' AS source_system,
						'src_cash_sales' AS source_entity
		FROM sa_cash_sales.src_cash_sales
		WHERE is_processed IS FALSE)
		INSERT INTO bl_3nf.ce_promotions 	(promotion_id,
											promotion_src_id,
											promotion_name,
											ta_insert_dt,
											ta_update_dt,
											source_system,
											source_entity)
		SELECT 	NEXTVAL('bl_3nf.promotion_id_seq'),
				COALESCE(promotion_id::INT, -1),
				COALESCE(promotion, 'n. a.'),
				NOW()::DATE,
				NOW()::DATE,
				source_system,
				source_entity
		FROM promotions_cte
		WHERE NOT EXISTS 	(SELECT 1
							FROM 	bl_3nf.ce_promotions p
							WHERE 	p.promotion_src_id = promotions_cte.promotion_id AND
									p.source_system = promotions_cte.source_system AND
									p.promotion_name = promotions_cte.promotion)
		ON CONFLICT ON CONSTRAINT ce_promotions_unique DO UPDATE
		SET promotion_name = EXCLUDED.promotion_name, ta_update_dt = CURRENT_DATE;
		GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
		WITH promotions_cte AS (
		SELECT DISTINCT promotion_id,
						promotion_name,
						'sa_card_sales' AS source_system,
						'src_card_sales' AS source_entity
		FROM sa_card_sales.src_card_sales
		WHERE is_processed IS FALSE)
		INSERT INTO bl_3nf.ce_promotions 	(promotion_id,
											promotion_src_id,
											promotion_name,
											ta_insert_dt,
											ta_update_dt,
											source_system,
											source_entity)
		SELECT 	NEXTVAL('bl_3nf.promotion_id_seq'),
				COALESCE(promotion_id::INT, -1),
				COALESCE(promotion_name, 'n. a.'),
				NOW()::DATE,
				NOW()::DATE,
				source_system,
				source_entity
		FROM promotions_cte
		WHERE NOT EXISTS 	(SELECT 1
							FROM 	bl_3nf.ce_promotions p
							WHERE 	p.promotion_src_id = promotions_cte.promotion_id AND
									p.source_system = promotions_cte.source_system AND
									p.promotion_name = promotions_cte.promotion_name)
		ON CONFLICT ON CONSTRAINT ce_promotions_unique DO UPDATE
		SET promotion_name = EXCLUDED.promotion_name, ta_update_dt = CURRENT_DATE;
		GET DIAGNOSTICS v_row_count_card = ROW_COUNT;
  						v_message = 'Success';
  						v_row_count = v_row_count_cash + v_row_count_card;
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

--CATEGORIES

CREATE OR REPLACE PROCEDURE bl_cl.add_categories_to_3nf()
AS $$
	DECLARE
		v_procedure_name VARCHAR(50);
		v_message VARCHAR(100);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;
BEGIN
	SELECT 'bl_cl.add_categories_to_3nf' INTO v_procedure_name;
   	WITH categories_cte AS (
   	SELECT DISTINCT category_id,
					category,
					'sa_cash_sales' AS source_system,
					'src_cash_sales' AS source_entity
	FROM sa_cash_sales.src_cash_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_categories 	(category_id,
								category_src_id,
								category_name,
								ta_insert_dt,
								ta_update_dt,
								source_system,
								source_entity)
	SELECT 	NEXTVAL('bl_3nf.category_id_seq'),
			COALESCE(category_id::INT, -1),
			COALESCE(category,'n. a.'),
			NOW()::DATE,
			NOW()::DATE,
			source_system,
			source_entity
	FROM categories_cte
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_categories cs
						WHERE 	cs.category_src_id = categories_cte.category_id AND
								cs.source_system = categories_cte.source_system);
	GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
	WITH categories_cte AS (
	SELECT DISTINCT category_id,
					category_name,
					'sa_card_sales' AS source_system,
					'src_card_sales' AS source_entity
	FROM sa_card_sales.src_card_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_categories 	(category_id,
								category_src_id,
								category_name,
								ta_insert_dt,
								ta_update_dt,
								source_system,
								source_entity)
	SELECT 	NEXTVAL('bl_3nf.category_id_seq'),
			COALESCE(category_id::INT, -1),
			COALESCE(category_name,'n. a.'),
			NOW()::DATE,
			NOW()::DATE,
			source_system,
			source_entity
	FROM categories_cte
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_categories cs
						WHERE 	cs.category_src_id = categories_cte.category_id AND
								cs.source_system = categories_cte.source_system);
		GET DIAGNOSTICS v_row_count_card = ROW_COUNT;
						v_row_count = v_row_count_cash + v_row_count_card;
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

--PRODUCTS

CREATE OR REPLACE PROCEDURE bl_cl.add_products_to_3nf()
AS $$
	DECLARE
		v_message VARCHAR(100);
		v_procedure_name VARCHAR(50);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;
BEGIN
	SELECT 'bl_cl.add_products_to_3nf' INTO v_procedure_name;
   	WITH products_cte AS (
   	SELECT DISTINCT product_id,
   					product_name,
					category_id,
					'sa_cash_sales' AS source_system,
					'src_cash_sales' AS source_entity
	FROM sa_cash_sales.src_cash_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_products 	(product_id,
									product_src_id,
									product_name,
									category_id,
									ta_insert_dt,
									ta_update_dt,
									source_system,
									source_entity)
	SELECT 	NEXTVAL('bl_3nf.product_id_seq'),
			COALESCE(product_id::INT, -1),
			COALESCE(product_name,'n. a.'),
			COALESCE(cc.category_id, -1),
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM products_cte cte
	LEFT JOIN bl_3nf.ce_categories cc ON cte.category_id = cc.category_src_id AND cte.source_system = cc.source_system 
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_products cp
						WHERE 	cp.product_src_id = cte.product_id AND
								cp.source_system = cte.source_system);
	GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
	WITH products_cte AS (
	SELECT DISTINCT product_id,
   					product_name,
					category_id,
					'sa_card_sales' AS source_system,
					'src_card_sales' AS source_entity
	FROM sa_card_sales.src_card_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_products 	(product_id,
									product_src_id,
									product_name,
									category_id,
									ta_insert_dt,
									ta_update_dt,
									source_system,
									source_entity)
	SELECT 	NEXTVAL('bl_3nf.product_id_seq'),
			COALESCE(product_id::INT, -1),
			COALESCE(product_name,'n. a.'),
			COALESCE(cc.category_id, -1),
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM products_cte cte
	LEFT JOIN bl_3nf.ce_categories cc ON cte.category_id = cc.category_src_id AND cte.source_system = cc.source_system 
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_products cp
						WHERE 	cp.product_src_id = cte.product_id AND
								cp.source_system = cte.source_system);
	GET DIAGNOSTICS v_row_count_card = ROW_COUNT;
					v_message = 'Success';
					v_row_count = v_row_count_cash + v_row_count_card;
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

--CITIES

CREATE OR REPLACE PROCEDURE bl_cl.add_cities_to_3nf()
AS $$
	DECLARE
		cur_data 	CURSOR FOR SELECT * FROM bl_cl.t_map_cities;
    	rec bl_cl.t_map_cities%ROWTYPE;
		v_message VARCHAR(100);
		v_procedure_name VARCHAR(50);
		v_row_count INT = 0;		
BEGIN
	SELECT 'bl_cl.add_cities_to_3nf' INTO v_procedure_name;
   	OPEN cur_data;
    LOOP
        FETCH NEXT FROM cur_data INTO rec;
       	EXIT WHEN NOT FOUND;
       	IF NOT EXISTS 	(SELECT 1
						FROM bl_3nf.ce_cities nf
						WHERE nf.city_name = rec.city_name)
		THEN
			BEGIN
		        INSERT INTO bl_3nf.ce_cities 	(city_id,
		        								city_src_id,
		        								city_name,
		        								ta_insert_dt,
		        								ta_update_dt,
		        								source_system, source_entity)
		        VALUES (NEXTVAL('bl_3nf.city_id_seq'),
		        		rec.city_id::VARCHAR,
		        		rec.city_name,
		        		NOW()::DATE,
		        		NOW()::DATE,
		        		'bl_cl', 't_map_cities');
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

--ADDRESSES

CREATE OR REPLACE PROCEDURE bl_cl.add_addresses_to_3nf()
AS $$
	DECLARE
		v_message VARCHAR(100);
		v_procedure_name VARCHAR(50);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;
BEGIN
	SELECT 'bl_cl.add_addresses_to_3nf' INTO v_procedure_name;
	---FIRST DATASET
	WITH addresses_cte AS (
	SELECT DISTINCT store_address_id, store_address, store_city_id,
					'sa_cash_sales' AS source_system, 'src_cash_sales' AS source_entity
					FROM sa_cash_sales.src_cash_sales
					WHERE is_processed IS FALSE
	UNION ALL
	SELECT DISTINCT customer_address_id, customer_address, customer_city_id,
					'sa_cash_sales' AS source_system, 'src_cash_sales' AS source_entity
					FROM sa_cash_sales.src_cash_sales
					WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_addresses  		(address_id,
											address_src_id, 
											address_street_address,
											city_id,
											ta_insert_dt,
											ta_update_dt,
											source_system,
											source_entity)
	SELECT 	NEXTVAL('bl_3nf.address_id_seq'),
			COALESCE(cte.store_address_id::INT, -1),
			COALESCE(cte.store_address, 'n. a.'),
			COALESCE(cnf.city_id::INT, -1),
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM addresses_cte cte
	LEFT JOIN bl_cl.t_map_cities tmc ON 	cte.store_city_id = tmc.city_src_id AND
											cte.source_system = tmc.source_system
	LEFT JOIN bl_3nf.ce_cities cnf ON tmc.city_id::VARCHAR = cnf.city_src_id
	WHERE NOT EXISTS 	(SELECT 1
						FROM bl_3nf.ce_addresses	
						WHERE 	cte.store_address_id = bl_3nf.ce_addresses.address_src_id AND
								cte.source_system = bl_3nf.ce_addresses.source_system);
	GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
	----SECOND DATASET
	WITH addresses_cte AS (
	SELECT DISTINCT store_address_id, NULL AS store_address, NULL AS store_city_id,
					'sa_card_sales' AS source_system, 'src_card_sales' AS source_entity
					FROM sa_card_sales.src_card_sales
					WHERE is_processed IS FALSE
	UNION ALL
	SELECT DISTINCT employee_address_id, employee_address, employee_city_id,
					'sa_card_sales' AS source_system, 'src_card_sales' AS source_entity
					FROM sa_card_sales.src_card_sales
					WHERE is_processed IS FALSE
	UNION ALL
	SELECT DISTINCT customer_address_id, customer_address, customer_city_id,
					'sa_card_sales' AS source_system, 'src_card_sales' AS source_entity
					FROM sa_card_sales.src_card_sales
					WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_addresses  		(address_id,
											address_src_id, 
											address_street_address,
											city_id,
											ta_insert_dt,
											ta_update_dt,
											source_system,
											source_entity)
	SELECT 	NEXTVAL('bl_3nf.address_id_seq'),
			COALESCE(cte.store_address_id::INT, -1),
			COALESCE(cte.store_address, 'n. a.'),
			COALESCE(cte.store_city_id::INT, -1),
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM addresses_cte cte
	LEFT JOIN bl_cl.t_map_cities tmc ON 	cte.store_city_id = tmc.city_src_id AND
											cte.source_system = tmc.source_system
	LEFT JOIN bl_3nf.ce_cities cnf ON tmc.city_id::VARCHAR = cnf.city_src_id
	WHERE NOT EXISTS 	(SELECT 1
						FROM bl_3nf.ce_addresses	
						WHERE 	cte.store_address_id = bl_3nf.ce_addresses.address_src_id AND
								cte.source_system = bl_3nf.ce_addresses.source_system);
	GET DIAGNOSTICS 	v_row_count_card = ROW_COUNT;
						v_row_count = v_row_count_cash + v_row_count_card;
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

--EMPLOYEES

CREATE OR REPLACE PROCEDURE bl_cl.add_employees_to_3nf()
AS $$
	DECLARE
		v_message VARCHAR(100);
		v_procedure_name VARCHAR(50);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;
BEGIN
	SELECT 'bl_cl.add_employees_to_3nf' INTO v_procedure_name;
	--CASH
	WITH employees_cte AS (
		SELECT DISTINCT employee_id, 
						employee_firstname,
						employee_secondname,
						NULL AS employee_gender,
						employee_birth,
						NULL AS employee_email,
						NULL AS employee_address_id,
						NULL AS employee_address,
						employee_city_id,
						employee_city,
						'sa_cash_sales' AS source_system,
						'src_cash_sales' AS source_entity
	FROM sa_cash_sales.src_cash_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_employees 	(employee_id,
										employee_src_id,
										employee_first_name,
										employee_last_name,
										employee_date_of_birth,
										employee_gender,
										employee_email,
										address_id,
										ta_insert_dt,
										ta_update_dt,
										source_system,
										source_entity)
	SELECT 	NEXTVAL('bl_3nf.employee_id_seq'),
			cte.employee_id,
			COALESCE(cte.employee_firstname, 'n. a.'),
			COALESCE(cte.employee_secondname, 'n. a.'),
			COALESCE(cte.employee_birth::DATE, date '1900-1-1'),
			COALESCE(cte.employee_gender, 'n.a.'),
			COALESCE(cte.employee_email, 'n.a.'),
			COALESCE(ca.address_id::INT, -1),			
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM employees_cte cte
	LEFT JOIN bl_3nf.ce_addresses ca ON 	cte.employee_address_id = ca.address_src_id AND
											cte.source_system = ca.source_system
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_employees cey
						WHERE 	cte.employee_id = cey.employee_src_id AND
								cte.source_system = cey.source_system);
	GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
	--CARD
	WITH employees_cte AS (
	SELECT DISTINCT employee_id, 
					NULL AS employee_firstname,
					employee_surname,
					employee_gender,
					employee_birth,
					employee_email,
					employee_address_id,
					employee_address,
					employee_city_id,
					employee_city,
					'sa_card_sales' AS source_system,
					'src_card_sales' AS source_entity
	FROM sa_card_sales.src_card_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_employees 	(employee_id,
										employee_src_id,
										employee_first_name,
										employee_last_name,
										employee_date_of_birth,
										employee_gender,
										employee_email,
										address_id,
										ta_insert_dt,
										ta_update_dt,
										source_system,
										source_entity)
	SELECT 	NEXTVAL('bl_3nf.employee_id_seq'),
			cte.employee_id,
			COALESCE(cte.employee_firstname, 'n. a.'),
			COALESCE(cte.employee_surname, 'n. a.'),
			COALESCE(cte.employee_birth::DATE, date '1900-1-1'),
			COALESCE(cte.employee_gender, 'n.a.'),
			COALESCE(cte.employee_email, 'n.a.'),
			COALESCE(ca.address_id::INT, -1),			
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM employees_cte cte
	LEFT JOIN bl_3nf.ce_addresses ca ON 	cte.employee_address_id = ca.address_src_id AND
											cte.source_system = ca.source_system
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_employees cey
						WHERE 	cte.employee_id = cey.employee_src_id AND
								cte.source_system = cey.source_system);
		GET DIAGNOSTICS 	v_row_count_card = ROW_COUNT;
							v_row_count = v_row_count_cash + v_row_count_card;
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

--STORES

CREATE OR REPLACE PROCEDURE bl_cl.add_stores_to_3nf()
AS $$
	DECLARE
		v_message VARCHAR(100);
		v_procedure_name VARCHAR(50);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;
BEGIN
	SELECT 'bl_cl.add_stores_to_3nf' INTO v_procedure_name;
	--CASH
	WITH stores_cte AS (	
	SELECT DISTINCT store_id,
					NULL AS store_name,
					store_address_id,
					'sa_cash_sales' AS source_system,
					'src_cash_sales' AS source_entity
	FROM sa_cash_sales.src_cash_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_stores 	(store_id,
									store_src_id,
									store_name,
									address_id,
									ta_insert_dt,
									ta_update_dt,
									source_system,
									source_entity)
	SELECT 	NEXTVAL('bl_3nf.store_id_seq'),
			cte.store_id,
			COALESCE(cte.store_name, 'n. a.'),
			ca.address_id,
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM stores_cte cte
	LEFT JOIN bl_3nf.ce_addresses ca ON 	cte.store_address_id::VARCHAR = ca.address_src_id AND 
											cte.source_system = ca.source_system
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_stores cs
						WHERE 	cte.store_id::VARCHAR = cs.store_src_id AND
								cte.source_system = cs.source_system);
	GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
	--CARD
	WITH stores_cte AS (	
	SELECT DISTINCT store_id,
					store_name,
					store_address_id,
					'sa_card_sales' AS source_system,
					'src_card_sales' AS source_entity
	FROM sa_card_sales.src_card_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_stores 	(store_id,
									store_src_id,
									store_name,
									address_id,
									ta_insert_dt,
									ta_update_dt,
									source_system,
									source_entity)
	SELECT 	NEXTVAL('bl_3nf.store_id_seq'),
			cte.store_id,
			COALESCE(cte.store_name, 'n. a.'),
			ca.address_id,
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM stores_cte cte
	LEFT JOIN bl_3nf.ce_addresses ca ON 	cte.store_address_id::VARCHAR = ca.address_src_id AND 
											cte.source_system = ca.source_system
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_stores cs
						WHERE 	cte.store_id::VARCHAR = cs.store_src_id AND
								cte.source_system = cs.source_system);
	GET DIAGNOSTICS v_row_count_card = ROW_COUNT;
					v_row_count = v_row_count_cash + v_row_count_card;
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

--CUSTOMERS

CREATE OR REPLACE PROCEDURE bl_cl.add_customers_to_3nf()
AS $$
	DECLARE
		v_message VARCHAR(100);
		v_procedure_name VARCHAR(50);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;
BEGIN
	SELECT 'bl_cl.add_customers_to_3nf' INTO v_procedure_name;
	WITH customers_cash_cte AS (	
	SELECT DISTINCT customer_id,
					customer_firstname,
					customer_surname,
					customer_gender,
					customer_birth,
					customer_email,
					customer_address_id,
					NULL AS end_dt,
					NULL AS is_active,
					'sa_cash_sales' AS source_system,
					'src_cash_sales' AS source_entity,
					is_processed
	FROM sa_cash_sales.src_cash_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_customers_scd 	(customer_id,
											start_dt,
											customer_src_id,
											customer_first_name,
											customer_last_name,
											customer_date_of_birth,
											customer_gender,
											customer_email,
											address_id,
											end_dt,
											is_active,
											ta_insert_dt,
											source_system,
											source_entity)
	SELECT 	NEXTVAL('bl_3nf.customer_id_seq'),
			'2017-01-01'::DATE,
			cash_cte.customer_id,
			cash_cte.customer_firstname,
			cash_cte.customer_surname,
			cash_cte.customer_birth::DATE,
			cash_cte.customer_gender,
			cash_cte.customer_email,
			ca.address_id,
			COALESCE(cash_cte.end_dt::DATE, date '9999-12-31'),
			COALESCE(cash_cte.is_active::BOOL, TRUE),
			NOW()::DATE,
			cash_cte.source_system,
			cash_cte.source_entity
	FROM customers_cash_cte cash_cte
	LEFT JOIN bl_3nf.ce_addresses ca ON 	cash_cte.customer_address_id = ca.address_src_id AND 
											cash_cte.source_system = ca.source_system
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_3nf.ce_customers_scd cus
						WHERE 	cash_cte.customer_id = cus.customer_src_id AND
								cus.source_system = 'sa_cash_sales' AND
								cash_cte.customer_firstname = cus.customer_first_name AND
								cash_cte.customer_surname = cus.customer_last_name AND
								cash_cte.customer_birth::DATE = cus.customer_date_of_birth AND
								cash_cte.customer_gender = cus.customer_gender AND
								cash_cte.customer_email = cus.customer_email);
	GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
	--CARD
	WITH customers_card_cte AS (	
	SELECT DISTINCT customer_id,
					customer_firstname,
					customer_surname,
					customer_gender,
					customer_birth,
					customer_email,
					customer_address_id,
					NULL AS end_dt,
					NULL AS is_active,
					'sa_card_sales' AS source_system,
					'src_card_sales' AS source_entity,
					is_processed
	FROM sa_card_sales.src_card_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_3nf.ce_customers_scd 	(customer_id,
											start_dt,
											customer_src_id,
											customer_first_name,
											customer_last_name,
											customer_date_of_birth,
											customer_gender,
											customer_email,
											address_id,
											end_dt,
											is_active,
											ta_insert_dt,
											source_system,
											source_entity)
	SELECT 	NEXTVAL('bl_3nf.customer_id_seq'),
			'2017-01-01'::DATE,
			card_cte.customer_id,
			card_cte.customer_firstname,
			card_cte.customer_surname,
			card_cte.customer_birth::DATE,
			card_cte.customer_gender,
			card_cte.customer_email,
			ca.address_id,
			COALESCE(card_cte.end_dt::DATE, date '9999-12-31'),
			COALESCE(card_cte.is_active::BOOL, TRUE),
			NOW()::DATE,
			card_cte.source_system,
			card_cte.source_entity
	FROM customers_card_cte card_cte
	LEFT JOIN bl_3nf.ce_addresses ca ON 	card_cte.customer_address_id = ca.address_src_id AND 
											card_cte.source_system = ca.source_system
	WHERE NOT EXISTS 	(SELECT 1
						FROM bl_3nf.ce_customers_scd cus
						WHERE 	card_cte.customer_id = cus.customer_src_id AND
								card_cte.source_system = cus.source_system AND
								card_cte.customer_firstname = cus.customer_first_name AND
								card_cte.customer_surname = cus.customer_last_name AND
								card_cte.customer_birth::DATE = cus.customer_date_of_birth AND
								card_cte.customer_gender = cus.customer_gender AND
								card_cte.customer_email = cus.customer_email);
	GET DIAGNOSTICS 	v_row_count_card = ROW_COUNT;
						v_row_count = v_row_count_cash + v_row_count_card;
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


--FACTS

CREATE OR REPLACE PROCEDURE bl_cl.add_facts_to_3nf()
AS $$
DECLARE
	v_row_count INT = 0;
   	v_message VARCHAR(5000);
   	v_procedure_name VARCHAR(5000);
BEGIN
	SELECT 'bl_cl.add_facts_to_3nf' INTO v_procedure_name;
	WITH fact_cte AS(
SELECT 	product_id,
		customer_id,
		store_id,
		employee_id,
		promotion_id,
		payment_method_id,
		date,
		quantity,
		revenue,
		regular_price,
		cost,
		profit,
		transaction_id,
		'sa_card_sales' AS source_system,
		'src_card_sales' AS source_entity
FROM sa_card_sales.src_card_sales
WHERE is_processed IS FALSE
UNION ALL
SELECT 	product_id,
		customer_id,
		store_id,
		employee_id,
		promotion_id,
		payment_method_id,
		day,
		quantity,
		revenue,
		regular_price,
		cost,
		amount,
		transaction_id,
		'sa_cash_sales' AS source_system,
		'src_cash_sales' AS source_entity
FROM sa_cash_sales.src_cash_sales
WHERE is_processed IS FALSE)
INSERT INTO bl_3nf.ce_sales	(product_id,
							customer_id,
							store_id,
							employee_id,
							promotion_id,
							payment_method_id,
							sale_transaction_id,
							event_dt,
							sale_quantity,
							sale_revenue,
							sale_regular_price,
							sale_cost,
							sale_amount,
							ta_insert_dt,
							ta_update_dt)
SELECT 	cp.product_id,
		cc.customer_id,
		cs.store_id,
		ce.employee_id,
		cp2.promotion_id,
		cpm.payment_method_id,
		cte.transaction_id::VARCHAR,
		cte.date::DATE,
		cte.quantity::DECIMAL,
		cte.revenue::DECIMAL,
		cte.regular_price::DECIMAL,
		cte.COST::DECIMAL,
		cte.profit::DECIMAL,
		NOW()::DATE,
		NOW()::DATE
FROM fact_cte cte
LEFT JOIN bl_3nf.ce_products cp ON cte.product_id = cp.product_src_id AND
							cte.source_system = cp.source_system
LEFT JOIN bl_3nf.ce_customers_scd cc ON cte.customer_id = cc.customer_src_id  AND
							 cte.source_system = cc.source_system AND cc.is_active IS TRUE
LEFT JOIN bl_3nf.ce_stores cs ON cte.store_id = cs.store_src_id AND
						  cte.source_system = cs.source_system	
LEFT JOIN bl_3nf.ce_employees ce ON cte.employee_id = ce.employee_src_id AND
							 cte.source_system = ce.source_system
LEFT JOIN bl_3nf.ce_promotions cp2 ON cte.promotion_id = cp2.promotion_src_id AND
								cte.source_system = cp2.source_system
LEFT JOIN bl_3nf.ce_payment_methods cpm ON cte.payment_method_id = cpm.payment_method_src_id AND 
									cte.source_system = cpm.source_system
WHERE NOT EXISTS 	(SELECT 1
					FROM 	bl_3nf.ce_sales f
					WHERE 	cc.customer_id::INT = f.customer_id AND cc.is_active IS FALSE AND
							cp.product_id::INT = f.product_id AND
							cs.store_id::INT = f.store_id AND
							ce.employee_id::INT = f.employee_id AND
							cp2.promotion_id::INT = f.promotion_id AND
							cpm.payment_method_id::INT = f.payment_method_id);
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
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










