--1. Создание в базе данных интеграционной таблицы для доступа к данным витрины v_sales_mart_<YYYYMMDD> в системе Greenplum.
-- DROP TABLE std9_121.ch_sales_mart_20210101_20210228_ext;
CREATE TABLE IF NOT EXISTS std9_121.ch_sales_mart_20210101_20210228_ext
(
    `plant_name`               String,
    `plant`                    String,
    `turnover`                 Float32,
    `sum_coupon_discount`      Float32,
    `turnover_with_discount`   Float32,
    `count_materials`          Float32,
    `bills_count`              Float32,
    `total_traffic`            Float32,
    `discount_material_count`  Float32,
    `discount_materials_share` Float32,
    `avg_materials`            Float32,
    `conversion`               Float32,
    `avg_bill`                 Float32,
    `avg_revenue_per_visitor`  Float32
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'v_sales_mart20210101_20210228', 'std9_121', 'password', 'login');
select * from std9_121.ch_sales_mart_20210101_20210228_ext;

-- Интеграционная таблица для доступа к данным витрины, сгруппированным по дням и по магазинам
-- DROP TABLE std9_121.ch_sales_data_ext;
CREATE TABLE IF NOT EXISTS std9_121.ch_sales_data_ext
(
    `calday`                  Date,
    `plant`                   String,
    `turnover`                Float32,
    `sum_coupon_discount`     Float32,
    `count_materials`         Float32,
    `bills_count`             Int32,
    `total_traffic`           Int32,
    `discount_material_count` Int32,
    `avg_bill`                Float32
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'sales_data_20210101_20210228', 'std9_121', 'password', 'login');
select * from std9_121.ch_sales_data_ext;


--2. Создание словарея на всех узлах кластера для доступа к данным таблицы stores в БД Greenplum:
drop dictionary std9_121.ch_stores_dict;
CREATE DICTIONARY IF NOT EXISTS std9_121.ch_stores_dict ON CLUSTER default_cluster (
    `plant` String,
    `txt` String
)
PRIMARY KEY plant
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std9_121' PASSWORD 'password' DB 'adb' TABLE 'std9_121.stores'))
LIFETIME(0)
LAYOUT(complex_key_hashed());

select * from std9_121.ch_stores_dict;
select * from system.dictionaries where name = 'ch_stores_dict' and database = 'std9_121';

--3. Создание реплицированных таблиц ch_sales_data на всех хостах кластера.
-- DROP TABLE std9_121.ch_sales_data;
CREATE TABLE IF NOT EXISTS std9_121.ch_sales_data ON CLUSTER default_cluster
(
    `calday`                  Date,
    `plant`                   String,
    `turnover`                Float32,
    `sum_coupon_discount`     Float32,
    `count_materials`         Float32,
    `bills_count`             Int32,
    `total_traffic`           Int32,
    `discount_material_count` Int32,
    `avg_bill`                Float32
)
ENGINE = ReplicatedMergeTree('/click/std9_121.ch_sales_data/{shard}', '{replica}')
ORDER BY (plant, toYYYYMMDD(calday));

SELECT * FROM std9_121.ch_sales_data;

SELECT * FROM system.clusters;
SELECT * FROM system.macros;


--4. Создание распределённой таблицы ch_plan_fact_distr, выбор для неё ключа шардирования.
--drop table std9_121.ch_sales_data_distr;
CREATE TABLE std9_121.ch_sales_data_distr AS std9_121.ch_sales_data
ENGINE = Distributed('default_cluster', 'std9_121', 'ch_sales_data', cityHash64(plant, toYYYYMMDD(calday)));

--Вставка всех записей из таблицы ch_plan_fact_ext.
INSERT INTO std9_121.ch_sales_data_distr SELECT * FROM std9_121.ch_sales_data_ext;

SELECT * FROM std9_121.ch_sales_data_distr;

SELECT * FROM std9_121.ch_sales_data;

SELECT shardNum(), COUNT(*)
FROM std9_121.ch_sales_data_distr
GROUP BY shardNum()
ORDER BY COUNT(*) DESC;

-- 5. Создание представления для отчета в Superset
CREATE OR REPLACE VIEW std9_121.v_ch_sales_report AS
SELECT
    dictGet('std9_121.ch_stores_dict', 'txt', plant) AS plant_name,
    plant,
    turnover,
    sum_coupon_discount,
    turnover - sum_coupon_discount AS turnover_with_discount,
    count_materials,
    bills_count,
    total_traffic,
    discount_material_count,
    discount_material_count / count_materials AS discount_materials_share,
    count_materials / bills_count AS avg_materials,
    bills_count / total_traffic AS conversion,
    turnover / bills_count AS avg_bill,
    turnover / total_traffic AS avg_revenue_per_visitor
FROM
(
    SELECT
        plant,
        SUM(turnover) AS turnover,
        SUM(sum_coupon_discount) AS sum_coupon_discount,
        SUM(count_materials) AS count_materials,
        SUM(bills_count) AS bills_count,
        SUM(total_traffic) AS total_traffic,
        SUM(discount_material_count) AS discount_material_count
    FROM std9_121.ch_sales_data_distr
    GROUP BY plant
) AS aggregated;

select * from std9_121.v_ch_sales_report;
