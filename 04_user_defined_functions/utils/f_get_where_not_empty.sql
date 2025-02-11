-- drop function if exists std9_121.f_get_where_not_empty(text);
create or replace function std9_121.f_get_where_not_empty(p_table text)
returns text
language plpgsql
as $$
DECLARE
	v_full_table_name text;
	v_table_name 	  text;
	v_schema_name 	  text;
    v_where			  text := ' ';
    r 			 	  record;
BEGIN
	v_full_table_name = std9_121.f_unify_name(p_name := p_table);
    v_schema_name = left(v_full_table_name, position('.' in v_full_table_name) - 1);
    v_table_name = right(v_full_table_name, length(v_full_table_name) - POSITION('.' in v_full_table_name));

    FOR r IN (SELECT column_name
              FROM information_schema.columns
              WHERE table_name = v_table_name
              AND table_schema = v_schema_name
			  AND data_type IN ('text', 'character varying', 'character')) LOOP
        IF v_where <> ' ' THEN
            v_where := v_where || 'AND ';
        END IF;
        v_where := v_where || r.column_name || ' != '''' ';
    END LOOP;

    RETURN v_where;
END;
$$;
execute on any;