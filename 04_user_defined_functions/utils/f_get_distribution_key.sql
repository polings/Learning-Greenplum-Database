--drop function std9_121.f_get_distribution_key(text);
create or replace function std9_121.f_get_distribution_key(p_table_name text)
	returns text
	language plpgsql
	volatile
as $$
DECLARE
	v_table_oid int4;
    v_dist_key  text;
BEGIN
	SELECT c.oid
    INTO v_table_oid
    FROM pg_class AS c
    INNER JOIN pg_namespace AS n ON c.relnamespace = n.oid
    WHERE n.nspname || '.' || c.relname = p_table_name
    LIMIT 1;

    IF v_table_oid = 0 OR v_table_oid IS NULL THEN
        v_dist_key := 'DISTRIBUTED RANDOMLY';
    ELSE
        v_dist_key := pg_get_table_distributedby(v_table_oid);
    END IF;

    RETURN v_dist_key;
END;
$$
execute on any;