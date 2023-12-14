DROP TABLE IF EXISTS bl_cl.t_map_cities;
CREATE TABLE bl_cl.t_map_cities AS SELECT * FROM bl_3nf.ce_cities OFFSET 1;

CREATE SEQUENCE IF NOT EXISTS bl_cl.t_map_city_id_seq
AS int
INCREMENT BY 1
START WITH 1
OWNED BY BL_CL.T_MAP_CITIES.CITY_ID;

CREATE OR REPLACE PROCEDURE bl_cl.add_cities_to_t_map()
AS $$
	DECLARE
		v_message VARCHAR(100);
		v_procedure_name VARCHAR(50);
		v_row_count INT;
		v_row_count_cash INT;
		v_row_count_card INT;		
BEGIN
	SELECT 'bl_cl.add_cities_to_t_map' INTO v_procedure_name;
   	WITH cities_cte AS (
   	SELECT DISTINCT customer_city_id,
   					customer_city,
					'sa_cash_sales' AS source_system,
					'src_cash_sales' AS source_entity
	FROM sa_cash_sales.src_cash_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_cl.t_map_cities 	(city_id,
									city_src_id,
									city_name,
									ta_insert_dt,
									ta_update_dt,
									source_system,
									source_entity)
	SELECT 	NEXTVAL('bl_cl.t_map_city_id_seq'),
			COALESCE(customer_city_id::INT, -1),
			COALESCE(customer_city, 'n. a.'),
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM cities_cte cte 
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_cl.t_map_cities ct
						WHERE 	ct.city_src_id = cte.customer_city_id AND
								ct.source_system = cte.source_system);
	GET DIAGNOSTICS v_row_count_cash = ROW_COUNT;
	WITH cities_cte AS (
   	SELECT DISTINCT customer_city_id,
   					customer_city,
					'sa_card_sales' AS source_system,
					'src_card_sales' AS source_entity
	FROM sa_card_sales.src_card_sales
	WHERE is_processed IS FALSE)
	INSERT INTO bl_cl.t_map_cities 	(city_id,
									city_src_id,
									city_name,
									ta_insert_dt,
									ta_update_dt,
									source_system,
									source_entity)
	SELECT 	NEXTVAL('bl_cl.t_map_city_id_seq'),
			COALESCE(customer_city_id::INT, -1),
			COALESCE(customer_city, 'n. a.'),
			NOW()::DATE,
			NOW()::DATE,
			cte.source_system,
			cte.source_entity
	FROM cities_cte cte 
	WHERE NOT EXISTS 	(SELECT 1
						FROM 	bl_cl.t_map_cities ct
						WHERE 	ct.city_src_id = cte.customer_city_id AND
								ct.source_system = cte.source_system);
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


