create or replace function std9_121.f_insert_table(p_table_from text, p_table_to text, p_where text DEFAULT NULL::text, p_truncate_tgt bool DEFAULT FALSE)
	returns int8
	language plpgsql
	volatile
as $$
declare
	v_table_from text;
	v_table_to text;
	v_where text;
	v_cnt int8;
begin
	-- Unify names
	v_table_from = std9_121.f_unify_name(p_name := p_table_from);
	v_table_to = std9_121.f_unify_name(p_name := p_table_to);

	v_where = COALESCE(p_where, ' 1 = 1 ');
	
	-- Write start log

	-- Truncate target table if needed
	if p_truncate_tgt is true then
		PERFORM std9_121.f_truncate_table(v_table_to);
	end if;
	
	-- Insert execution
	execute 'INSERT INTO '||v_table_to||' SELECT * FROM '||v_table_from||' WHERE '||v_where;

	get diagnostics v_cnt = ROW_COUNT;
	raise notice '% rows inserted from % into %', v_cnt, v_table_from, v_table_to;
	
	-- Write end log

	return v_cnt;
end;
$$
execute on any;