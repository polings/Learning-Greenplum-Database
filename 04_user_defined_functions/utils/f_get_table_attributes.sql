--drop function std9_121.f_get_table_attributes(text);
create or replace function std9_121.f_get_table_attributes(p_table_name text)
	returns text
	language plpgsql
	volatile
as $$
DECLARE
	v_params text;
BEGIN
	SELECT COALESCE('WITH (' || ARRAY_TO_STRING(reloptions, ', ') || ')', '')
    FROM pg_class
    WHERE oid = p_table_name::REGCLASS
    INTO v_params;
    RETURN v_params;
END;
$$
execute on any;