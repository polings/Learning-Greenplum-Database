--Для таблиц фактов реализована загрузка DELTA_PARTITION - полная подмена партиций.
--drop function std9_121.f_load_delta_partitions(text, text, text, timestamp, timestamp);
create or replace function std9_121.f_load_delta_partitions(p_table_from text, p_table_to text, p_partition_key text,
                                                			p_start_date timestamp, p_end_date timestamp)
    returns int8
    language plpgsql
    security definer
    volatile
as
$$
DECLARE
	v_location 		text := 'std9_121.f_load_delta_partitions';
    v_table_from    text;
    v_table_to      text;
    v_start_date    date;
    v_end_date      date;
    v_load_interval interval;
    v_iterDate      timestamp;
    v_where         text;
    v_prt_table     text;
    v_cnt           int8 := 0;
	v_cnt_prt		int8 := 0;
BEGIN
    -- Unify names
    v_table_from = std9_121.f_unify_name(p_name := p_table_from);
    v_table_to = std9_121.f_unify_name(p_name := p_table_to);

    -- Write start log
    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
			                          p_log_message := 'Start loading partitions for ' || v_table_to ||' from '||v_table_from,
			                          p_location 	:= v_location);

    -- Checking if source table is not empty
    EXECUTE 'SELECT COUNT(1) FROM (SELECT * FROM ' || v_table_from || ' LIMIT 1) cnt' into v_cnt;
    IF v_cnt = 0 THEN
        -- Write end log
			PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
					                     p_log_message := 'End loading partitions for '|| v_table_to ||' from '|| v_table_from ||', source table is empty, 0 records loaded',
					                     p_location    := v_location);
        RETURN 0;
    END IF;

    -- Insert data
    PERFORM std9_121.f_create_date_partitions(v_table_to, p_end_date);

    v_load_interval = '1 month'::interval;
    v_start_date := DATE_TRUNC('month', p_start_date);
    v_end_date := DATE_TRUNC('month', p_end_date) + v_load_interval;
    LOOP
        v_iterDate = v_start_date + v_load_interval;
        EXIT WHEN (v_iterDate > v_end_date);

        v_prt_table = std9_121.f_create_tmp_table(p_table_name := v_table_to, p_prefix_name := 'prt_',
                                                  p_suffix_name := '_' || to_char(v_start_date, 'YYYYMMDD'));
        v_where = p_partition_key || '>=''' || v_start_date || '''::timestamp and ' || p_partition_key || '<''' || v_iterDate || '''::timestamp';

		v_cnt_prt = std9_121.f_insert_table(p_table_from := v_table_from, p_table_to := v_prt_table, p_where := v_where, p_truncate_tgt := FALSE);
        v_cnt = v_cnt + v_cnt_prt;

		EXECUTE  'ALTER TABLE ' || v_table_to || ' EXCHANGE PARTITION FOR (DATE ''' || v_start_date || ''') WITH TABLE ' || v_prt_table || ' WITH VALIDATION;';
		EXECUTE 'DROP TABLE ' || v_prt_table;

        v_start_date := v_iterDate;
    END LOOP;

    -- Analyze the table
    EXECUTE std9_121.f_analyze_table(v_table_to);

    -- Write end log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := 'End loading partitions for '|| v_table_to ||' from '|| v_table_from,
                                 p_location    := v_location);
    RETURN v_cnt;
END;
$$
execute on any;