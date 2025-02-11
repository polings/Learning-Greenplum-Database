--Функция для расчёта витрины, которая содержит результат выполнения плана продаж в разрезе:
--         Код "Региона",
--         Код "Товарного направления" (matdirec),
--         Код "Канала сбыта",
--         Плановое количество,
--         Фактические количество,
--         Процент выполнения плана за месяц,
--         Код самого продаваемого товара в регионе.
--Функция также создает представление (VIEW) на основе созданной витрины.
select * from std9_121.f_load_mart('202101');

--drop function std9_121.f_load_mart(p_table_from text, p_table_to text, p_truncate_tgt bool);
CREATE OR REPLACE FUNCTION std9_121.f_load_mart(p_month varchar)
RETURNS int4
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_table_name text;
	v_start_date timestamp;
	v_end_date timestamp;
	v_params text;
	v_dist_key text;
	v_sql_mart text;
    v_sql text;
	v_view_name text;
    v_return int;
BEGIN
    v_table_name = std9_121.f_unify_name(p_name := 'plan_fact_' || p_month);

    PERFORM std9_121.f_write_log(p_log_type 	:= 'INFO',
                                  	  p_log_message := 'Start f_load_mart',
                                  	  p_location 	:= 'Sales mart calculation');

	SELECT date_trunc('month', to_date(p_month, 'YYYYMM')) INTO v_start_date;
	SELECT (v_start_date + INTERVAL '1 month' - INTERVAL '1 day') INTO v_end_date;

	v_params = 'WITH (
        appendonly=true,
        orientation=column,
        compresstype=zstd,
        compresslevel=1
    )';
	v_sql_mart := format(
		'WITH month_sales as (
		    SELECT * FROM std9_121.sales s
		    WHERE s.date BETWEEN %L AND %L
		),
		sales_quantity AS (
		    SELECT s.region, p.matdirec, s.distr_chan, SUM(s.quantity) AS sales_qt
		    FROM month_sales s
		    JOIN std9_121.product p ON s.material = p.material
		    GROUP BY 1, 2, 3
		),
		plan_quantity AS (
		    SELECT p.region, p.matdirec, p.distr_chan, SUM(p.quantity) AS plan_qt
		    FROM std9_121.plan p
		    WHERE p.date BETWEEN %L AND %L
		    GROUP BY 1, 2, 3
		),
		rank_material AS (
		    SELECT region, s.material, RANK() OVER (PARTITION BY region ORDER BY SUM(quantity) DESC) AS rnk
		    FROM month_sales s
		    JOIN std9_121.product p ON s.material = p.material
		    GROUP BY 1, 2
		)
		SELECT pq.region, pq.matdirec, pq.distr_chan, coalesce(sq.sales_qt, 0) AS sales_qt, coalesce(pq.plan_qt, 0) AS plan_qt, coalesce(sq.sales_qt * 100.0 / NULLIF(pq.plan_qt, 0), 0) AS plan_exec_perc, rm.material
		FROM plan_quantity pq
		LEFT JOIN sales_quantity sq ON sq.region = pq.region AND sq.matdirec = pq.matdirec AND sq.distr_chan = pq.distr_chan
		JOIN rank_material rm ON pq.region = rm.region AND rm.rnk = 1',
        v_start_date, v_end_date, v_start_date, v_end_date
    );
	v_dist_key = 'DISTRIBUTED BY (material)';
    v_sql := format(
        'DROP TABLE IF EXISTS %I CASCADE;
        CREATE TABLE %I %s AS %s %s;',
        v_table_name, v_table_name, v_params, v_sql_mart, v_dist_key
    );
	EXECUTE v_sql;

    EXECUTE format('SELECT COUNT(1) FROM %I', v_table_name) INTO v_return;

    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := v_return || ' rows inserted',
                                 p_location    := 'Sales mart calculation');

    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := 'End f_load_mart',
                                 p_location    := 'Sales mart calculation');

	v_view_name = std9_121.f_unify_name(p_name := 'v_' || v_table_name);
	v_sql := format(
        'DROP VIEW IF EXISTS %I;
        CREATE OR REPLACE VIEW %I AS (
	    SELECT 
	        pf.region, 
	        r.txt AS region_name, 
	        pf.matdirec, 
	        pf.distr_chan, 
	        c.txtsh AS chan_name, 
	        pf.plan_exec_perc, 
	        pf.material, 
	        p.brand, 
	        p.txt AS product_name, 
	        p2.price
	    FROM %I pf
	    JOIN std9_121.region r ON pf.region = r.region
	    JOIN std9_121.chanel c ON pf.distr_chan = c.dist_chan
	    JOIN std9_121.product p ON pf.material = p.material
	    LEFT JOIN std9_121.price p2 ON pf.material = p2.material AND pf.region = p2.region AND pf.distr_chan = p2.dist_chan);',
        v_view_name, v_view_name, v_table_name
    );
	EXECUTE v_sql;

    RETURN v_return;
END;
$$
EXECUTE ON ANY;
