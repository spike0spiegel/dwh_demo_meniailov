CREATE OR REPLACE FUNCTION bl_cl.customers_scd2()
RETURNS TRIGGER
AS $$
DECLARE
		v_customer_id INT;
		v_customer_src_id VARCHAR(30);
		v_customer_first_name VARCHAR(30);
		v_customer_last_name VARCHAR(30);
		v_customer_date_of_birth DATE;
		v_customer_gender VARCHAR(5);
		v_customer_email VARCHAR(50);
		v_address_id INT;
		v_source_system varchar(100);
		v_source_entity varchar(30);
		v_end_dt DATE;
		v_date_name VARCHAR(5);
BEGIN
	IF NEW.source_system = 'sa_cash_sales' THEN v_date_name:= 'day';
	ELSE v_date_name:= 'date';
	END IF;
	--
	IF EXISTS 	(SELECT 1 
				FROM bl_3nf.ce_customers_scd c
				WHERE 	c.customer_src_id = NEW.customer_src_id AND
						LOWER(c.source_system) = LOWER(new.source_system)
						AND end_dt = '9999-12-31' AND
						(c.customer_first_name <> NEW.customer_first_name OR
						c.customer_last_name <> NEW.customer_last_name OR
						c.customer_date_of_birth <> NEW.customer_date_of_birth OR
						c.customer_gender <> NEW.customer_gender OR 
						c.customer_email <> NEW.customer_email OR
						c.address_id <> NEW.address_id)) THEN
	--end_dt
	EXECUTE 'SELECT MAX('|| v_date_name ||')::DATE
			FROM ' || quote_ident(NEW.source_system) || '.' || quote_ident(NEW.source_entity) || '
			WHERE customer_id = $1
			AND is_processed IS TRUE'
			INTO v_end_dt
	USING NEW.customer_src_id;
	v_end_dt:= v_end_dt + INTERVAL '1 DAY';
		UPDATE bl_3nf.ce_customers_scd c
		SET is_active = FALSE, end_dt = v_end_dt
		WHERE c.customer_src_id = NEW.customer_src_id AND
	LOWER(c.source_system) = LOWER(new.source_system)
	AND end_dt = '9999-12-31' AND
		(c.customer_first_name <> NEW.customer_first_name OR
		c.customer_last_name <> NEW.customer_last_name OR
		c.customer_date_of_birth <> NEW.customer_date_of_birth OR
		c.customer_gender <> NEW.customer_gender OR 
		c.customer_email <> NEW.customer_email OR
		c.address_id <> NEW.address_id)
	RETURNING 	customer_id, customer_src_id, customer_first_name, customer_last_name, customer_date_of_birth, customer_gender,
							customer_email, address_id, source_system, source_entity
	INTO 	v_customer_id, v_customer_src_id, v_customer_first_name, v_customer_last_name, v_customer_date_of_birth, v_customer_gender, 
			v_customer_email, v_address_id, v_source_system, v_source_entity;
	INSERT INTO bl_3nf.ce_customers_scd	(customer_id,
									customer_src_id,
									start_dt,
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
	VALUES (v_customer_id, NEW.customer_src_id, v_end_dt, NEW.customer_first_name,
			NEW.customer_last_name, NEW.customer_date_of_birth, NEW.customer_gender,
			NEW.customer_email, NEW.address_id, date '9999-12-31', TRUE, NOW()::DATE, NEW.source_system, NEW.source_entity);
	RETURN NULL;
	ELSE RETURN NEW;
	END IF;
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_customers_scd2 ON bl_3nf.ce_customers_scd;
CREATE TRIGGER trigger_customers_scd2
BEFORE INSERT ON bl_3nf.ce_customers_scd
FOR EACH ROW WHEN (pg_trigger_depth() < 1) EXECUTE PROCEDURE bl_cl.customers_scd2();

CREATE OR REPLACE FUNCTION bl_cl.customers_scd2_dm()
RETURNS TRIGGER
AS $$
DECLARE
		v_customer_id INT;
		v_customer_first_name VARCHAR(30);
		v_customer_last_name VARCHAR(30);
		v_customer_date_of_birth DATE;
		v_customer_gender VARCHAR(5);
		v_customer_email VARCHAR(50);
		v_address_id INT;
		v_address VARCHAR;
		v_city_id INT;
		v_city_name VARCHAR;
		v_source_system varchar(100);
		v_source_entity varchar(30);
		v_end_dt DATE;
		v_date_name VARCHAR(5);
BEGIN
	SELECT end_dt
	INTO v_end_dt
	FROM bl_3nf.ce_customers_scd
	WHERE is_active = FALSE AND customer_id::VARCHAR = NEW.customer_src_id;
	--
	SELECT 	customer_first_name, customer_last_name, customer_date_of_birth, customer_gender,
			customer_email, ccs.address_id, ca.address_street_address, ca.city_id, ci.city_name
	INTO 	v_customer_first_name, v_customer_last_name, v_customer_date_of_birth, v_customer_gender,
			v_customer_email, v_address_id, v_address, v_city_id, v_city_name
	FROM bl_3nf.ce_customers_scd ccs
	LEFT JOIN bl_3nf.ce_addresses ca ON ccs.address_id = ca.address_id 
	LEFT JOIN bl_3nf.ce_cities ci ON ca.city_id = ci.city_id
	WHERE customer_id::VARCHAR = NEW.customer_src_id AND is_active IS TRUE AND customer_src_id <> 'n. a.';
	--
	INSERT INTO bl_dm.dim_customers_scd (customer_surr_id,	customer_src_id,		customer_first_name,
										customer_last_name,	customer_date_of_birth,	customer_gender,
										customer_email,		customer_address_id,	customer_street_address,
										customer_city_id,	customer_city_name,		start_dt,
										end_dt,				is_active,				ta_insert_dt,
										source_system,		source_entity)
		VALUES  (NEXTVAL('bl_dm.seq_dim_customers_scd'),	COALESCE(NEW.customer_src_id, 'n. a.'),						COALESCE(v_customer_first_name,'n. a.'),
				COALESCE(v_customer_last_name,'n. a.'),		COALESCE(v_customer_date_of_birth::DATE, date '1900-1-1'), 	COALESCE(v_customer_gender,'n. a.'),
				COALESCE(v_customer_email, 'n. a.'),		COALESCE(v_address_id, -1),									COALESCE(v_address,'n. a.'),
				COALESCE(v_city_id, -1),					COALESCE(v_city_name, 'n. a.'),								COALESCE(v_end_dt, date '1900-1-1'),
				DATE '9999-12-31', 							TRUE,														NOW()::DATE,
				'bl_3nf',									'ce_customers_scd, ce_addresses, ce_cities');
	--
	UPDATE bl_dm.dim_customers_scd cu
	SET end_dt = v_end_dt
	WHERE cu.customer_src_id = NEW.customer_src_id AND is_active = FALSE;
	RETURN NULL;
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_customers_scd2_dm ON bl_dm.dim_customers_scd;
CREATE TRIGGER trigger_customers_scd2_dm
AFTER UPDATE ON bl_dm.dim_customers_scd
FOR EACH ROW WHEN (pg_trigger_depth() < 1) EXECUTE PROCEDURE bl_cl.customers_scd2_dm();
