-- SQL запрос к таблице в Clickhouse с использованием функций словарей-справочников для дальнейшего создания дашборда.
-- Формируется аналогичное представление, как в Greenplum (v_plan_fact), с результатами выполнения плана продаж,
-- текстами для кодов и информацией о самом продаваемом товаре в регионе.
SELECT
    region,
    dictGet('std9_121.ch_region_dict', 'txt', region) AS region_name,
    matdirec,
    distr_chan,
    dictGet('std9_121.ch_chanel_dict', 'txtsh', distr_chan) AS chan_name,
    sales_qt,
    plan_qt,
    plan_exec_perc,
    material,
    dictGet('std9_121.ch_product_dict', 'brand', material) AS brand,
    dictGet('std9_121.ch_product_dict', 'txt', material) AS product_name,
    dictGet('std9_121.ch_price_dict', 'price', material) AS price
FROM std9_121.ch_plan_fact_distr