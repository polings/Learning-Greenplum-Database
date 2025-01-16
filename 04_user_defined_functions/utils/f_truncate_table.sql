create or replace function std9_121.f_truncate_table(p_table_name text)
	returns void
	language plpgsql
	volatile
as $$
declare
	v_table_name text;
	v_sql text;
begin
	-- Unify name
	v_table_name := std9_121.f_unify_name(p_name := p_table_name);
	
	-- Write start log
	
	-- Truncate execution
	v_sql := 'TRUNCATE TABLE '||v_table_name;
	execute v_sql;

	-- Write end log

end;
$$
execute on any;