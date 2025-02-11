-- Пользовательская функция для загрузки справочников в таблицы:
-- Загрузка данных в целевые таблицы производится из внешних таблиц.
-- Реализована FULL загрузка (полная очистка целевой таблицы и полная вставка всех записей).

-- drop function std9_121.f_full_load(p_table_from text, p_table_to text, p_truncate_tgt bool);
create or replace function std9_121.f_full_load(p_table_from text, p_table_to text, p_truncate_tgt bool DEFAULT FALSE)
	returns int8
	language plpgsql
	security definer
	volatile
as $$
DECLARE
	v_location 	 text := 'std9_121.f_full_load';
	v_table_from text;
	v_table_to 	 text;
	v_where 	 text;
	v_cnt 		 int8;
BEGIN
	-- Unify names
	v_table_from = std9_121.f_unify_name(p_name := p_table_from);
	v_table_to = std9_121.f_unify_name(p_name := p_table_to);

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
			                          p_log_message := 'START Switch table '||v_table_to||' with table '||v_table_from,
			                          p_location 	:= v_location);

	-- Checking if source table is empty
	EXECUTE 'SELECT COUNT(1) FROM (SELECT * FROM '||v_table_from||' LIMIT 1) cnt' into v_cnt;
	IF v_cnt = 0 THEN
		-- Write end log
	    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
				                          p_log_message := 'END Switch table '||v_table_to||' with table '||v_table_from||', source table is empty',
				                          p_location 	:= v_location);
		RETURN 0;
	END IF;

	-- Generate the condition
	v_where = std9_121.f_get_where_not_empty(v_table_to);

	-- Insert data
	v_cnt = std9_121.f_insert_table(p_table_from := v_table_from, p_table_to := v_table_to, p_where := v_where, p_truncate_tgt := p_truncate_tgt);

	-- Analyze the table
	EXECUTE std9_121.f_analyze_table(v_table_to);

	-- Write end log
    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
			                          p_log_message := 'END Switch table '||v_table_to||' with table '||v_table_from,
			                          p_location 	:= v_location);

	RETURN v_cnt;
END;
$$
execute on any;