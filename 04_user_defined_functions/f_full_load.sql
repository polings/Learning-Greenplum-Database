-- 1. Создайте 2 пользовательские функции для загрузки данных в таблицы: 
-- Загрузка данных в целевые таблицы должна производиться из внешних таблиц.
-- Первая функция для загрузки справочников, вторая - для загрузки таблиц фактов.
-- Для таблиц справочников необходимо реализовать FULL загрузку (полная очистка целевой таблицы и полная вставка всех записей).

drop function std9_121.f_full_load(p_table_from text, p_table_to text, p_where text DEFAULT NULL::text, p_truncate_tgt bool DEFAULT FALSE);
create or replace function std9_121.f_full_load(p_table_from text, p_table_to text, p_where text DEFAULT NULL::text, p_truncate_tgt bool DEFAULT FALSE)
	returns int8
	language plpgsql
	security definer
	volatile
as $$
DECLARE
	v_table_from text;
	v_table_to text;
	v_where text;
	v_cnt int8;
BEGIN
	-- Unify names
	v_table_from = std9_121.f_unify_name(p_name := p_table_from);
	v_table_to = std9_121.f_unify_name(p_name := p_table_to);

	-- Write start log

	-- Checking if source table is empty
	EXECUTE 'SELECT COUNT(1) FROM (SELECT * FROM '||v_table_from||' LIMIT 1) cnt' into v_cnt;
	IF v_cnt = 0 THEN
		-- Write log later
		RETURN 0;
	END IF;
	
	-- Insert data
	v_cnt = std9_121.f_insert_table(p_table_from := v_table_from, p_table_to := v_table_to, p_where := p_where, p_truncate_tgt := p_truncate_tgt);

	-- Analyze the table
	EXECUTE std9_121.f_analyze_table(v_table_to);

	-- Write end log

	RETURN v_cnt;
END;
$$
execute on any;

-- Fill the tables
select * 
from std9_121.f_full_load(
	p_table_from := 'std9_121.ext_price', 
	p_table_to := 'std9_121.price', 
	p_where := ' material != '''' AND region != '''' AND dist_chan != ''''', 
	p_truncate_tgt := true
);

select * 
from std9_121.f_full_load(
	p_table_from := 'std9_121.ext_chanel', 
	p_table_to := 'std9_121.chanel',
	p_truncate_tgt := true
);

select * 
from std9_121.f_full_load(
	p_table_from := 'std9_121.ext_product', 
	p_table_to := 'std9_121.product',
	p_truncate_tgt := true
);

select * 
from std9_121.f_full_load(
	p_table_from := 'std9_121.ext_region', 
	p_table_to := 'std9_121.region',
	p_truncate_tgt := true
);

-- ToDo
-- Для таблиц фактов можно реализовать загрузку следующими способами:
-- DELTA_PARTITION - полная подмена партиций.
-- DELTA_UPSERT - предварительное удаление по ключу и последующая вставка записей из временной таблицы в целевую.


