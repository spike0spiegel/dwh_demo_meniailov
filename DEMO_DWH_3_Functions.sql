CREATE OR REPLACE PROCEDURE bl_cl.logger(i_procedure_name VARCHAR(30), i_affected_rows INT, i_event_date TIMESTAMPTZ, i_message VARCHAR(30))
AS $$
DECLARE v_procedure_name VARCHAR(500):= i_procedure_name;
		v_affected_rows INT:= i_affected_rows;
		v_event_date TIMESTAMPTZ:= i_event_date;
		v_message VARCHAR(500):= i_message;
BEGIN
	INSERT INTO bl_cl.logging (procedure_name, affected_rows, event_date, message)
	VALUES (v_procedure_name, v_affected_rows, v_event_date, v_message);
END;
$$
LANGUAGE plpgsql; 

CREATE OR REPLACE FUNCTION bl_cl.load_cash_data_from_ext_to_src	()
RETURNS INT
AS $$
DECLARE
		v_error_message VARCHAR(100);
		v_row_count INT;
BEGIN
	INSERT INTO sa_cash_sales.src_cash_sales
	SELECT * FROM sa_cash_sales.ext_cash_sales ext
	WHERE NOT EXISTS 	(SELECT 1
						FROM sa_cash_sales.src_cash_sales src
						WHERE src.id = ext.id);
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	RETURN v_row_count;
EXCEPTION
        WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
    RAISE NOTICE 'Error: %', v_error_message;
   	RAISE;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bl_cl.load_card_data_from_ext_to_src()
RETURNS INT
AS $$
DECLARE
		v_error_message VARCHAR(100);
		v_row_count INT;
BEGIN
	INSERT INTO sa_card_sales.src_card_sales
	SELECT * FROM sa_card_sales.ext_card_sales ext
	WHERE NOT EXISTS 	(SELECT 1
						FROM sa_card_sales.src_card_sales src
						WHERE src.id = ext.id);
	GET DIAGNOSTICS v_row_count = ROW_COUNT;
	RETURN v_row_count;
EXCEPTION
        WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_message = MESSAGE_TEXT;
    RAISE NOTICE 'Error: %', v_error_message;
   	RAISE;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE bl_cl.load_data_from_ext_to_src()
AS $$
DECLARE
		v_row_count_cash INT;
		v_row_count_card INT;
		v_row_count INT;
		v_message VARCHAR;
		v_procedure_name VARCHAR;
BEGIN
	SELECT 'bl_cl.load_data_from_ext_to_src' INTO v_procedure_name;
	SELECT * FROM bl_cl.load_cash_data_from_ext_to_src() INTO v_row_count_cash;
	SELECT * FROM bl_cl.load_card_data_from_ext_to_src() INTO v_row_count_card;
	v_row_count = v_row_count_cash + v_row_count_card;
	v_message = 'Success';
	--
	CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	--
    EXCEPTION
        WHEN OTHERS THEN
           	GET STACKED DIAGNOSTICS v_message = MESSAGE_TEXT;      
			RAISE WARNING 'Error occured: %', v_message;
			ROLLBACK;
			CALL bl_cl.logger(v_procedure_name, v_row_count, NOW(), v_message);
	COMMIT;
END;
$$
LANGUAGE plpgsql;

