create or replace function std9_121.f_create_pxf_table(p_table text, p_pxf_table text, p_user_id text, p_pass text)
	returns void
	language plpgsql
	volatile
as $$
DECLARE
	v_location  text := 'std9_121.f_create_pxf_table';
	v_ext_table text;
	v_sql       text;
    v_pxf       text;
BEGIN
	-- Unify name
	v_ext_table := std9_121.f_unify_name(p_name := p_table || '_ext');

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := 'START creating external table '||v_ext_table || ' with pxf',
                                 p_location    := v_location);
	-- PXF connection parameters
    v_pxf := 'pxf://' || p_pxf_table || '?PROFILE=Jdbc&JDBC_DRIVER=org.postgresql.Driver' ||
             '&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres' ||
             '&USER=' || p_user_id || '&PASS=' || p_pass;
    RAISE NOTICE 'PXF CONNECTION STRING: %', v_pxf;

	-- Creating external table
	EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table;
    v_sql := 'CREATE EXTERNAL TABLE ' || v_ext_table || ' (LIKE ' || p_table || ')
              LOCATION (''' || v_pxf || ''')
              ON ALL
              FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_import'')
              ENCODING ''UTF8''';
    EXECUTE v_sql;

	-- Write end log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := 'END creating external table '||v_ext_table || ' with pxf',
                                 p_location    := v_location);
END;
$$
execute on any;