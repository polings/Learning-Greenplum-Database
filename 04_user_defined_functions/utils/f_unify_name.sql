create or replace function std9_121.f_unify_name(p_name text)
	returns text
	language plpgsql
	volatile
as $$
begin
	return lower(trim(translate(p_name, ';/''', '')));
end;
$$
execute on any;