-- drop function if exists std9_121.f_truncate_table(text);
create or replace function std9_121.f_truncate_table(p_table_name text)
	returns void
	language plpgsql
	volatile
as $$
DECLARE
	v_location   text := 'std9_121.f_truncate_table';
	v_table_name text;
	v_sql		 text;
BEGIN
	-- Unify name
	v_table_name := std9_121.f_unify_name(p_name := p_table_name);

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
			                          p_log_message := 'START truncate table '||v_table_name,
			                          p_location 	:= v_location);

	-- Truncate execution
	v_sql := 'TRUNCATE TABLE '||v_table_name;
	EXECUTE v_sql;

	-- Write end log
    PERFORM std9_121.f_write_log(p_log_type 	 := 'INFO',
                                       p_log_message := 'END truncate table '||v_table_name,
                                       p_location    := v_location);

END;
$$
execute on any;