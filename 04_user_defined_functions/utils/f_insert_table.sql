-- drop function if exists std9_121.f_insert_table(text, text, text, bool);
create or replace function std9_121.f_insert_table(p_table_from text, p_table_to text, p_where text DEFAULT NULL::text, p_truncate_tgt bool DEFAULT FALSE)
	returns int8
	language plpgsql
	volatile
as $$
DECLARE
	v_location 	 text := 'std9_121.f_insert_table_sql';
	v_table_from text;
	v_table_to   text;
	v_where 	 text;
	v_cnt		 int8;
	v_sql		 text;
BEGIN
	-- Unify names
	v_table_from = std9_121.f_unify_name(p_name := p_table_from);
	v_table_to = std9_121.f_unify_name(p_name := p_table_to);

	v_where = COALESCE(p_where, ' 1 = 1 ');

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
			                     p_log_message := 'START Insert data from table '||v_table_from||' to '||v_table_to || ' with condition: '||v_where,
			                     p_location    := v_location);

	-- Truncate target table if needed
	IF p_truncate_tgt IS TRUE THEN
		PERFORM std9_121.f_truncate_table(v_table_to);
	END IF;

	-- Insert the data
	v_sql := 'INSERT INTO '||v_table_to||' SELECT * FROM '||v_table_from||' WHERE '||v_where;
	EXECUTE v_sql;

	GET DIAGNOSTICS v_cnt = ROW_COUNT;
	RAISE NOTICE '% rows inserted from % into %', v_cnt, v_table_from, v_table_to;

	-- Write end log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
	                         	 p_log_message := 'END Insert data from table '||v_table_from||' to '||v_table_to||', '||v_cnt||' rows inserted',
	                          	 p_location    := v_location);

	RETURN v_cnt;
END;
$$
execute on any;