-- Отчет по продажам с учетом скидок
SELECT std9_121.f_load_sales_mart('2021-01-01', '2021-02-28');

-- drop function std9_121.f_load_sales_mart(date, date);
CREATE OR REPLACE FUNCTION std9_121.f_load_sales_mart(p_start_date date, p_end_date date DEFAULT NULL::date)
RETURNS int4
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_location 	 text := 'std9_121.f_load_sales_mart';
    v_table_name text;
	v_start_date date;
	v_end_date   date;
	v_params     text;
	v_dist_key   text;
	v_sql_mart   text;
    v_sql        text;
	v_view_name  text;
    v_return     int;
BEGIN
    v_table_name := 'sales_report_' || TO_CHAR(p_start_date, 'YYYYMMDD') || COALESCE('_' || TO_CHAR(p_end_date, 'YYYYMMDD'), '');
    v_table_name := std9_121.f_unify_name(p_name := v_table_name);
    v_start_date := p_start_date;
    v_end_date := COALESCE(p_end_date, v_start_date);

    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := 'Start sales mart loading',
                                 p_location    := v_location);
	v_params = 'WITH (
        appendonly=true,
        orientation=column,
        compresstype=zstd,
        compresslevel=1
    )';
	v_sql_mart := format(
		    'WITH discounts AS (
                SELECT dc.plant, SUM(dc.discount) AS sum_coupon_discount, COUNT(*) AS discount_material_count
                FROM std9_121.dds_coupons dc
                WHERE dc.calday BETWEEN %L AND %L
                GROUP BY dc.plant
            ),
            bills AS (
                SELECT db.plant, SUM(db.rpa_sat) AS turnover,
                       SUM(db.qty) AS count_materials,
                       COUNT(DISTINCT db.billnum) AS bills_count,
                       SUM(db.qty) / COUNT(DISTINCT db.billnum) AS avg_materials,
                       SUM(db.rpa_sat) / COUNT(DISTINCT db.billnum) AS avg_bill
                FROM std9_121.dds_bills db
                WHERE db.calday BETWEEN %L AND %L
                GROUP BY db.plant
            ),
            traffic AS (
                SELECT ot.plant, SUM(ot.quantity) AS total_traffic
                FROM ods_traffic ot
                WHERE ot.calday BETWEEN %L AND %L
                GROUP BY ot.plant
            )
            SELECT d.plant,
                   b.turnover,
                   d.sum_coupon_discount,
                   b.count_materials,
                   b.bills_count,
                   t.total_traffic,
                   d.discount_material_count,
                   b.avg_materials,
                   b.avg_bill
            FROM discounts d
            JOIN bills b ON d.plant = b.plant
            JOIN traffic t ON d.plant = t.plant',
        v_start_date, v_end_date, v_start_date, v_end_date, v_start_date, v_end_date
    );
	v_dist_key = 'DISTRIBUTED BY (plant)';
    v_sql := format(
        'DROP TABLE IF EXISTS %I CASCADE;
        CREATE TABLE %I %s AS %s %s;',
        v_table_name, v_table_name, v_params, v_sql_mart, v_dist_key
    );
	EXECUTE v_sql;

    EXECUTE format('SELECT COUNT(1) FROM %I', v_table_name) INTO v_return;
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := v_return || ' rows inserted',
                                 p_location    := v_location);

	v_view_name = std9_121.f_unify_name(p_name := 'v_' || v_table_name);
	v_sql := format(
        'DROP VIEW IF EXISTS %I;
         CREATE OR REPLACE VIEW %I AS (
                SELECT s2.txt AS "plant_name",
                       s1.plant,
                       s1.turnover,
                       s1.sum_coupon_discount,
                       s1.turnover - s1.sum_coupon_discount AS turnover_with_discount,
                       s1.count_materials,
                       s1.bills_count,
                       s1.total_traffic,
                       s1.discount_material_count,
                       ROUND(s1.discount_material_count / 0.01 / NULLIF(s1.count_materials, 0), 1) AS discount_materials_share,
                       ROUND(s1.avg_materials, 2) AS avg_materials,
                       ROUND(s1.bills_count / 0.01 / NULLIF(s1.total_traffic, 0), 2) AS conversion,
                       ROUND(s1.avg_bill, 1) AS avg_bill,
                       ROUND(s1.turnover / NULLIF(s1.total_traffic, 0), 1) AS avg_revenue_per_visitor
                FROM %I s1
                JOIN std9_121.stores s2 ON s1.plant = s2.plant);',
         v_view_name, v_view_name, v_table_name
    );
	EXECUTE v_sql;

    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                 p_log_message := 'End f_load_mart',
                                 p_location    := v_location);
    RETURN v_return;
END;
$$
EXECUTE ON ANY;



--
--
-- EXPLAIN ANALYZE
-- WITH discounts AS (
--     SELECT dc.plant, SUM(dc.discount) AS sum_coupon_discount, COUNT(*) AS discount_material_count
--     FROM std9_121.dds_coupons dc
--     WHERE dc.calday BETWEEN '2021-01-01' AND '2021-02-28'
--     GROUP BY dc.plant
-- ),
-- bills AS (
--     SELECT db.plant, SUM(db.rpa_sat) AS turnover,
--            SUM(db.qty) AS count_materials,
--            COUNT(DISTINCT db.billnum) AS bills_count,
--            SUM(db.qty) / COUNT(DISTINCT db.billnum) AS avg_materials,
--            SUM(db.rpa_sat) / COUNT(DISTINCT db.billnum) AS avg_bill
--     FROM std9_121.dds_bills db
--     WHERE db.calday BETWEEN '2021-01-01' AND '2021-02-28'
--     GROUP BY db.plant
-- ),
-- traffic AS (
--     SELECT ot.plant, SUM(ot.quantity) AS total_traffic
--     FROM ods_traffic ot
--     WHERE ot.calday BETWEEN '2021-01-01' AND '2021-02-28'
--     GROUP BY ot.plant
-- )
-- SELECT d.plant,
--        b.turnover,
--        d.sum_coupon_discount,
--        b.count_materials,
--        b.bills_count,
--        t.total_traffic,
--        d.discount_material_count,
--        b.avg_materials,
--        b.avg_bill
-- FROM discounts d
-- JOIN bills b ON d.plant = b.plant
-- JOIN traffic t ON d.plant = t.plant;




--
-- WITH discounts AS (
--     SELECT dc.plant, SUM(dc.discount) AS sum_coupon_discount, COUNT(*) AS discount_material_count
--     FROM std9_121.dds_coupons dc
--     WHERE dc.calday BETWEEN '2021-01-01' AND '2021-02-28'
--     GROUP BY dc.plant
-- ),
-- bills AS (
--     SELECT db.plant, SUM(db.rpa_sat) AS turnover,
--            SUM(db.qty) AS count_materials,
--            COUNT(DISTINCT db.billnum) AS bills_count,
--            SUM(db.qty) / COUNT(DISTINCT db.billnum) AS avg_materials,
--            SUM(db.rpa_sat) / COUNT(DISTINCT db.billnum) AS avg_bill
--     FROM std9_121.dds_bills db
--     WHERE db.calday BETWEEN '2021-01-01' AND '2021-02-28'
--     GROUP BY db.plant
-- ),
-- traffic AS (
--     SELECT ot.plant, SUM(ot.quantity) AS total_traffic
--     FROM ods_traffic ot
--     WHERE ot.calday BETWEEN '2021-01-01' AND '2021-02-28'
--     GROUP BY ot.plant
-- )
-- SELECT d.plant,
--        b.turnover,
--        d.sum_coupon_discount,
--        b.turnover - d.sum_coupon_discount AS turnover_with_discount,
--        b.count_materials,
--        b.bills_count,
--        t.total_traffic,
--        d.discount_material_count,
--        ROUND(d.discount_material_count / 0.01 / NULLIF(b.count_materials, 0), 1) AS discount_materials_share,
--        ROUND(b.avg_materials, 2),
--        ROUND(b.bills_count / 0.01 / NULLIF(t.total_traffic, 0), 2) AS conversion,
--        ROUND(b.avg_bill, 1),
--        ROUND(b.turnover / NULLIF(t.total_traffic, 0), 1) AS avg_revenue_per_visitor
-- FROM discounts d
-- JOIN bills b ON d.plant = b.plant
-- JOIN traffic t ON d.plant = t.plant;