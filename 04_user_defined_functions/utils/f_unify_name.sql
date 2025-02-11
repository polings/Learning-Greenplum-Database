-- drop function if exists std9_121.f_unify_name(text);
create or replace function std9_121.f_unify_name(p_name text)
	returns text
	language plpgsql
	volatile
as $$
BEGIN
	RETURN lower(trim(translate(p_name, ';/''', '')));
END;
$$
execute on any;