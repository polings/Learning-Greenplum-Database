--drop function std9_121.f_create_tmp_table(text, text, text);
create or replace function std9_121.f_create_tmp_table(p_table_name text, p_prefix_name text, p_suffix_name text)
    returns text
    language plpgsql
    volatile
as
$$
DECLARE
	v_location		  text := 'std9_121.f_create_tmp_table';
    v_table_name      text;
    v_full_table_name text;
    v_tmp_t_name      text;
    v_storage_param   text;
    v_sql             text;
    v_schema_name     text;
    v_dist_key        text;
BEGIN
    -- Get parameters
    v_table_name = std9_121.f_unify_name(p_name := p_table_name);
    v_full_table_name = std9_121.f_unify_name(p_name := p_table_name);
    v_schema_name = left(v_full_table_name, position('.' in v_full_table_name) - 1);
    v_table_name = right(v_full_table_name, length(v_full_table_name) - POSITION('.' in v_full_table_name));
    v_tmp_t_name = v_schema_name || '.' || p_prefix_name || v_table_name || p_suffix_name;
    v_tmp_t_name = std9_121.f_unify_name(p_name := v_tmp_t_name);

    v_storage_param = std9_121.f_get_table_attributes(p_table_name := v_full_table_name);
    v_dist_key = std9_121.f_get_distribution_key(p_table_name := v_full_table_name);

    -- Write start log
    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
			                          p_log_message := 'START Creating temp table ' || v_tmp_t_name || ' for table ' || v_full_table_name,
			                          p_location 	:= v_location);

    v_sql := 'CREATE TABLE ' || v_tmp_t_name || ' (LIKE '|| v_full_table_name || ') ' || v_storage_param||' '||v_dist_key ||';';
    EXECUTE v_sql;

    -- Write end log
    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
			                          p_log_message := 'END Creating temp table '||v_tmp_t_name||' for table '||v_full_table_name,
			                          p_location 	:= v_location);

    RETURN v_tmp_t_name;
END;
$$
execute on any;