--drop function std9_121.f_create_date_partitions(text, timestamp);
create or replace function std9_121.f_create_date_partitions(p_table_name text, p_partition_value timestamp)
    returns void
    language plpgsql
    volatile
as
$$
DECLARE
	v_location 			text := 'std9_121.f_create_date_partitions';
    v_cnt_partitions    int;
    v_table_name        text;
    v_partition_end_sql text;
    v_partition_end     timestamp;
    v_interval          interval;
    v_ts_format         text := 'YYYY-MM-DD HH24:MI:SS';
BEGIN
    -- Unify name
    v_table_name := std9_121.f_unify_name(p_name := p_table_name);

    -- Write start log
    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
			                          p_log_message := 'START Creating partitions for table ' || v_table_name,
			                          p_location 	:= v_location);

    -- Check partitions existing
    SELECT COUNT(*) INTO v_cnt_partitions FROM pg_partitions p WHERE p.schemaname || '.' || p.tablename = lower(v_table_name);

    IF v_cnt_partitions > 1 THEN
        LOOP
            -- Get the last partition parameters
            SELECT partitionrangeend INTO v_partition_end_sql
                FROM (SELECT p.*, RANK() OVER (ORDER BY partitionrank DESC) rnk
                      FROM pg_partitions p WHERE p.partitionrank IS NOT NULL AND p.schemaname || '.' || p.tablename = lower(v_table_name)) q
                WHERE rnk = 1;

            -- End date the last partition
            EXECUTE 'SELECT ' || v_partition_end_sql INTO v_partition_end;

            -- Check if the partition already exists for p_partition_value
            EXIT WHEN v_partition_end > p_partition_value;

            -- Cut the new partition from default partition, if it doesn't exist
            v_interval := '1 month'::interval;
            EXECUTE 'ALTER TABLE ' || v_table_name || ' SPLIT DEFAULT PARTITION START (' || v_partition_end_sql ||
                    ') END (''' || to_char(v_partition_end + v_interval, v_ts_format) || '''::timestamp);';
			-- Write end loop log
		    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
					                          p_log_message := 'Created partition ' || v_partition_end_sql || ' for table ' || v_table_name,
					                          p_location 	:= v_location);
        END LOOP;
	ELSE
	    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
				                          p_log_message := 'Table is not partitioned ' || v_table_name,
				                          p_location 	:= v_location);
	END IF;

    -- Write end log
    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
			                          p_log_message := 'END Created partitions for table ' || v_table_name,
			                          p_location 	:= v_location);
END ;
$$
execute on any;