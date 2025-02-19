-- drop function std9_121.f_full_load_promos(p_table_from text, p_table_to text, p_truncate_tgt bool);
create or replace function std9_121.f_full_load_promos(p_table_from text, p_table_to text, p_truncate_tgt bool DEFAULT FALSE)
	returns int8
	language plpgsql
	security definer
	volatile
as $$
DECLARE
	v_location 	 text := 'std9_121.f_full_load_promos';
	v_table_from text;
	v_table_to 	 text;
	v_where 	 text;
    v_sql        text;
	v_cnt 		 int8;
BEGIN
	-- Unify names
	v_table_from = std9_121.f_unify_name(p_name := p_table_from);
	v_table_to = std9_121.f_unify_name(p_name := p_table_to);

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
			                     p_log_message := 'START Switch table '||v_table_to||' with table '||v_table_from,
			                     p_location    := v_location);

	-- Checking if source table is empty
	EXECUTE 'SELECT COUNT(1) FROM (SELECT * FROM '||v_table_from||' LIMIT 1) cnt' into v_cnt;
	IF v_cnt = 0 THEN
		-- Write end log
	    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                     p_log_message := 'END Switch table '||v_table_to||' with table '||v_table_from||', source table is empty',
                                     p_location    := v_location);
		RETURN 0;
	END IF;

	-- Truncate target table if needed
	IF p_truncate_tgt IS TRUE THEN
		PERFORM std9_121.f_truncate_table(v_table_to);
	END IF;

	-- Generate the condition
	v_where = COALESCE(std9_121.f_get_where_not_empty(v_table_to), ' 1 = 1 ');

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
			                     p_log_message := 'START Insert data from table '||v_table_from||' to '||v_table_to || ' with condition: '||v_where,
			                     p_location    := v_location);
	-- Insert data
	v_sql := 'INSERT INTO '||v_table_to||' SELECT promo_id, txt, promo_type, material::BIGINT, discount FROM '||v_table_from||' WHERE '||v_where;
	EXECUTE v_sql;

	GET DIAGNOSTICS v_cnt = ROW_COUNT;
	RAISE NOTICE '% rows inserted from % into %', v_cnt, v_table_from, v_table_to;

	-- Write end log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
	                         	 p_log_message := 'END Insert data from table '||v_table_from||' to '||v_table_to||', '||v_cnt||' rows inserted',
	                          	 p_location    := v_location);

	-- Analyze the table
	EXECUTE std9_121.f_analyze_table(v_table_to);

	-- Write end log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
			                     p_log_message := 'END Switch table '||v_table_to||' with table '||v_table_from,
			                     p_location    := v_location);

	RETURN v_cnt;
END;
$$
execute on any;