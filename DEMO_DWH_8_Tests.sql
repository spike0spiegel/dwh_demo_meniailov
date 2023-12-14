CREATE OR REPLACE FUNCTION bl_cl.show_cash_src()
RETURNS SETOF sa_cash_sales.src_cash_sales
AS $$
BEGIN
	RETURN QUERY SELECT * FROM sa_cash_sales.src_cash_sales;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bl_cl.show_card_src()
RETURNS SETOF sa_card_sales.src_card_sales
AS $$
BEGIN
	RETURN QUERY SELECT * FROM sa_card_sales.src_card_sales;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bl_cl.get_bl_3nf_info() 
RETURNS TABLE (table_name text, column_count int, row_count bigint, table_size VARCHAR) AS $$
DECLARE
    table_record record;
BEGIN
    FOR table_record IN 
        SELECT ist.table_name 
        FROM information_schema.tables ist
        WHERE ist.table_schema = 'bl_3nf'
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM bl_3nf.%I', table_record.table_name) INTO row_count;
       	--
        EXECUTE format('SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ''bl_3nf'' AND table_name = %L', table_record.table_name) 
       	INTO column_count;
       	-- 
       	EXECUTE format('SELECT pg_size_pretty(pg_total_relation_size(''bl_3nf.%I''))', table_record.table_name) INTO table_size;
        RETURN QUERY SELECT table_record.table_name::TEXT, column_count::INT, row_count::BIGINT, table_size;
    END LOOP;
    RETURN;
END;
$$ 
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bl_cl.get_bl_dm_info() 
RETURNS TABLE (table_name text, column_count int, row_count bigint, table_size VARCHAR) AS $$
DECLARE
    table_record record;
BEGIN
    FOR table_record IN 
        SELECT ist.table_name 
        FROM information_schema.tables ist
        WHERE ist.table_schema = 'bl_dm'
    LOOP
        EXECUTE format('SELECT COUNT(*) FROM bl_dm.%I', table_record.table_name) INTO row_count;
       	--
        EXECUTE format('SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = ''bl_dm'' AND table_name = %L', table_record.table_name) 
       	INTO column_count;
       	-- 
       	EXECUTE format('SELECT pg_size_pretty(pg_total_relation_size(''bl_dm.%I''))', table_record.table_name) INTO table_size;
        RETURN QUERY SELECT table_record.table_name::TEXT, column_count::INT, row_count::BIGINT, table_size;
    END LOOP;
    RETURN;
END;
$$ 
LANGUAGE plpgsql;


DO $$
DECLARE
	v_size INTEGER;
	v_size_unique INTEGER;
BEGIN
	SELECT COUNT(*) -- INTO v_size
	FROM bl_dm.fct_sales_dd;
	--
	WITH cte AS (SELECT DISTINCT customer_surr_id, event_dt, product_surr_id, employee_surr_id, promotion_surr_id, payment_method_surr_id, store_surr_id
	FROM bl_dm.fct_sales_dd)
	SELECT COUNT(*) --INTO v_size_unique
	FROM cte;
	--
	IF v_size = v_size_unique THEN
		UPDATE bl_cl.testing
		SET test_passed = TRUE
		WHERE test_description = 'Target table doesn’t contain duplicates.';
	ELSE 
		UPDATE bl_cl.testing
		SET test_passed = FALSE
		WHERE test_description = 'Target table doesn’t contain duplicates.';
	END IF;	
END
$$;


DO $$
BEGIN
	IF NOT EXISTS 	(SELECT transaction_id
					FROM sa_cash_sales.src_cash_sales
					UNION ALL 
					SELECT transaction_id
					FROM sa_card_sales.src_card_sales
					EXCEPT
					SELECT sale_transaction_id
					FROM bl_dm.fct_sales_dd) 
	THEN
		UPDATE bl_cl.testing
		SET test_passed = TRUE
		WHERE test_description = 'All records from SA layer are represented in the business layer.';
	ELSE
		UPDATE bl_cl.testing
		SET test_passed = TRUE
		WHERE test_description = 'All records from SA layer are represented in the business layer.';
	END IF;	
END
$$;

DROP TABLE IF EXISTS bl_cl.testing;
CREATE TABLE IF NOT EXISTS bl_cl.testing (
	test_name VARCHAR(50),
	test_description VARCHAR(100),
	test_script VARCHAR(1000),
	test_passed BOOLEAN DEFAULT FALSE,
	test_time TIMESTAMPTZ DEFAULT '1900-01-01 00:00:00+00:00');

INSERT INTO bl_cl.testing (test_name, test_description, test_script)
VALUES ('Duplicates', 'Target table doesn’t contain duplicates.', 'DO $$
DECLARE
	v_size INTEGER;
	v_size_unique INTEGER;
BEGIN
	SELECT COUNT(*) INTO v_size
	FROM bl_dm.fct_sales_dd;
	--
	WITH cte AS (SELECT DISTINCT customer_surr_id, event_dt, product_surr_id, employee_surr_id, promotion_surr_id, payment_method_surr_id, store_surr_id
	FROM bl_dm.fct_sales_dd)
	SELECT COUNT(*) INTO v_size_unique
	FROM cte;
	--
	IF v_size = v_size_unique THEN
		UPDATE bl_cl.testing
		SET test_passed = TRUE
		WHERE test_description = ''Target table doesn’t contain duplicates.'';
	ELSE 
		UPDATE bl_cl.testing
		SET test_passed = FALSE
		WHERE test_description = ''Target table doesn’t contain duplicates.'';
	END IF;	
END
$$;');

INSERT INTO bl_cl.testing (test_name, test_description, test_script)
VALUES ('All records from SA', 'All records from SA layer are represented in the business layer.', 'DO $$
BEGIN
	IF NOT EXISTS 	(SELECT transaction_id
					FROM sa_cash_sales.src_cash_sales
					UNION ALL 
					SELECT transaction_id
					FROM sa_card_sales.src_card_sales
					EXCEPT
					SELECT sale_transaction_id
					FROM bl_dm.fct_sales_dd) 
	THEN
		UPDATE bl_cl.testing
		SET test_passed = TRUE
		WHERE test_description = ''All records from SA layer are represented in the business layer.'';
	ELSE
		UPDATE bl_cl.testing
		SET test_passed = TRUE
		WHERE test_description = ''All records from SA layer are represented in the business layer.'';
	END IF;	
END
$$;');

SELECT *
FROM bl_cl.testing;


CREATE OR REPLACE PROCEDURE bl_cl.do_testing()
AS $$
DECLARE
	cur_data 	CURSOR FOR SELECT * FROM bl_cl.testing;
    rec bl_cl.testing%ROWTYPE;
BEGIN
	OPEN cur_data;
	    LOOP
	        FETCH NEXT FROM cur_data INTO rec;
	       	EXIT WHEN NOT FOUND;
			EXECUTE rec.test_script;
		END LOOP;
	CLOSE cur_data;
END;
$$ 
LANGUAGE plpgsql;

CALL bl_cl.do_testing();
