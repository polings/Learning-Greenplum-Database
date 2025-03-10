-- Отчет по продажам с учетом скидок
SELECT std9_121.f_load_sales_mart('2021-01-01', '2021-02-28');

-- SELECT gp_segment_id, COUNT(*) AS row_count
-- FROM std9_121.sales_data_20210101_20210228
-- GROUP BY gp_segment_id
-- ORDER BY row_count DESC;

-- drop function std9_121.f_load_sales_mart(date, date);
CREATE OR REPLACE FUNCTION std9_121.f_load_sales_mart(p_start_date date, p_end_date date DEFAULT NULL::date)
RETURNS int4
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    v_location 	 text := 'std9_121.f_load_sales_mart';
    v_table_name text;
    v_suffix     text;
	v_start_date date;
	v_end_date   date;
	v_params     text;
	v_dist_key   text;
	v_sql_mart   text;
    v_sql        text;
	v_view_name  text;
    v_return     int;
BEGIN
    v_suffix := TO_CHAR(p_start_date, 'YYYYMMDD') || COALESCE('_' || TO_CHAR(p_end_date, 'YYYYMMDD'), '');
    v_table_name := 'sales_data_' || v_suffix;
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
                    SELECT dc.calday, dc.plant, SUM(dc.discount) AS sum_coupon_discount, COUNT(*) AS discount_material_count
                    FROM std9_121.dds_coupons dc
                    WHERE dc.calday BETWEEN %L AND %L
                    GROUP BY dc.calday, dc.plant
                ),
                bills AS (
                    SELECT db.calday, db.plant, SUM(db.rpa_sat) AS turnover,
                           SUM(db.qty) AS count_materials,
                           COUNT(DISTINCT db.billnum) AS bills_count,
                           SUM(db.qty) / COUNT(DISTINCT db.billnum) AS avg_materials,
                           SUM(db.rpa_sat) / COUNT(DISTINCT db.billnum) AS avg_bill
                    FROM std9_121.dds_bills db
                    WHERE db.calday BETWEEN %L AND %L
                    GROUP BY db.calday, db.plant
                ),
                traffic AS (
                    SELECT ot.calday , ot.plant, SUM(ot.quantity) AS total_traffic
                    FROM ods_traffic ot
                    WHERE ot.calday BETWEEN %L AND %L
                    GROUP BY ot.calday, ot.plant
                )
            SELECT COALESCE(t.calday, b.calday) AS calday,
                   COALESCE(t.plant, b.plant) AS plant,
                   COALESCE(b.turnover, 0) AS turnover,
                   COALESCE(d.sum_coupon_discount, 0) AS sum_coupon_discount,
                   COALESCE(b.count_materials, 0) AS count_materials,
                   COALESCE(b.bills_count, 0) AS bills_count,
                   t.total_traffic,
                   COALESCE(d.discount_material_count, 0) AS discount_material_count,
                   COALESCE(b.avg_bill, 0) AS avg_bill
            FROM bills b
            LEFT JOIN discounts d ON d.plant = b.plant AND d.calday = b.calday
            RIGHT JOIN traffic t ON t.plant = b.plant AND t.calday = b.calday',
        v_start_date, v_end_date, v_start_date, v_end_date, v_start_date, v_end_date
    );
	v_dist_key = 'DISTRIBUTED RANDOMLY';
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

	v_view_name = std9_121.f_unify_name(p_name := 'v_sales_mart' || v_suffix);
	v_sql := format(
        'DROP VIEW IF EXISTS %I;
         CREATE OR REPLACE VIEW %I AS (
                SELECT s.txt AS plant_name,
                       sr.plant,
                       SUM(sr.turnover) AS turnover,
                       SUM(sr.sum_coupon_discount) AS sum_coupon_discount,
                       SUM(sr.turnover) - SUM(sr.sum_coupon_discount) AS turnover_with_discount,
                       SUM(sr.count_materials) AS count_materials,
                       SUM(sr.bills_count) AS bills_count,
                       SUM(sr.total_traffic) AS total_traffic,
                       SUM(sr.discount_material_count) AS discount_material_count,
                       ROUND(SUM(sr.discount_material_count) / 0.01 / SUM(sr.count_materials), 1) AS discount_materials_share,
                       ROUND(SUM(sr.count_materials) / SUM(sr.bills_count), 2) AS avg_materials,
                       ROUND(SUM(sr.bills_count) / 0.01 / SUM(sr.total_traffic), 2) AS conversion,
                       ROUND(SUM(sr.turnover) / SUM(sr.bills_count), 1) AS avg_bill,
                       ROUND(SUM(sr.turnover) / SUM(sr.total_traffic), 1) AS avg_revenue_per_visitor
                FROM %I sr
                JOIN std9_121.stores s ON sr.plant = s.plant
                GROUP BY s.txt, sr.plant);',
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


EXPLAIN ANALYZE
WITH discounts AS (
        SELECT dc.calday, dc.plant, SUM(dc.discount) AS sum_coupon_discount, COUNT(*) AS discount_material_count
        FROM std9_121.dds_coupons dc
        WHERE dc.calday BETWEEN '2021-01-01' AND '2021-02-28'
        GROUP BY dc.calday, dc.plant
    ),
    bills AS (
        SELECT db.calday, db.plant, SUM(db.rpa_sat) AS turnover,
               SUM(db.qty) AS count_materials,
               COUNT(DISTINCT db.billnum) AS bills_count,
               SUM(db.qty) / COUNT(DISTINCT db.billnum) AS avg_materials,
               SUM(db.rpa_sat) / COUNT(DISTINCT db.billnum) AS avg_bill
        FROM std9_121.dds_bills db
        WHERE db.calday BETWEEN '2021-01-01' AND '2021-02-28'
        GROUP BY db.calday, db.plant
    ),
    traffic AS (
        SELECT ot.calday , ot.plant, SUM(ot.quantity) AS total_traffic
        FROM ods_traffic ot
        WHERE ot.calday BETWEEN '2021-01-01' AND '2021-02-28'
        GROUP BY ot.calday, ot.plant
    )
SELECT COALESCE(t.calday, b.calday) AS calday,
       COALESCE(t.plant, b.plant) AS plant,
       COALESCE(b.turnover, 0) AS turnover,
       COALESCE(d.sum_coupon_discount, 0) AS sum_coupon_discount,
       COALESCE(b.count_materials, 0) AS count_materials,
       COALESCE(b.bills_count, 0) AS bills_count,
       t.total_traffic,
       COALESCE(d.discount_material_count, 0) AS discount_material_count,
       COALESCE(b.avg_materials, 0) AS avg_materials,
       COALESCE(b.avg_bill, 0) AS avg_bill
FROM bills b
LEFT JOIN discounts d ON d.plant = b.plant AND d.calday = b.calday
RIGHT JOIN traffic t ON t.plant = b.plant AND t.calday = b.calday;