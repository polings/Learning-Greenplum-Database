-- DROP FUNCTION f_create_gpfdist_table(text,text,text);
CREATE OR REPLACE FUNCTION std9_121.f_create_gpfdist_table(p_table text, p_file_name text, p_format_param text)
RETURNS BOOLEAN
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_location  text := 'std9_121.f_create_gpfdist_table';
    v_ext_table text;
    v_sql       text;
    v_gpfdist   text;
    v_exists    boolean;
BEGIN
	-- Unify name
	v_ext_table := std9_121.f_unify_name(p_name := p_table || '_ext');

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := 'START creating external table ' || v_ext_table || ' with gpfdist',
                                 p_location    := v_location);
    -- GPFDIST connection parameters
    v_gpfdist = 'gpfdist://172.16.128.10:8080/' || p_file_name || '.csv';

	-- Creating external table
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;
    v_sql = 'CREATE EXTERNAL TABLE ' || v_ext_table ||
            '(LIKE ' || p_table || ')
            LOCATION (''' || v_gpfdist || ''')
            ON ALL
            FORMAT ' || p_format_param || '
            ENCODING ''UTF8''
	        SEGMENT REJECT LIMIT 5 ROWS;';
	EXECUTE v_sql;

    -- Check if the external table was successfully created
    SELECT EXISTS (
        SELECT 1 FROM pg_tables p WHERE p.schemaname || '.' || p.tablename = lower(v_ext_table)
    ) INTO v_exists;

    -- Log Success or Failure
    IF v_exists THEN
        PERFORM std9_121.f_write_log(
            p_log_type    := 'INFO',
            p_log_message := 'SUCCESS: External table ' || v_ext_table || ' created successfully with gpfdist',
            p_location    := v_location
        );
        RETURN TRUE;
    ELSE
        PERFORM std9_121.f_write_log(
            p_log_type    := 'ERROR',
            p_log_message := 'FAILED: External table ' || v_ext_table || ' was not created with gpfdist',
            p_location    := v_location
        );
        RETURN FALSE;
    END IF;
END;
$$;