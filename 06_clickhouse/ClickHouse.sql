--1. Создание базы данных.
CREATE DATABASE std9_121;
select * from system.databases; 


--2. Создание в базе данных интеграционной таблицы ch_plan_fact_ext для доступа к данным витрины plan_fact_<YYYYMM> в системе Greenplum.
-- DROP TABLE std9_121.ch_plan_fact_ext;
CREATE TABLE IF NOT EXISTS std9_121.ch_plan_fact_ext
(
    `region` String,
    `matdirec` String,
    `distr_chan` String,
    `sales_qt` Int32,
    `plan_qt` Int32,
    `plan_exec_perc` Float32,
    `material` String
)
ENGINE = PostgreSQL('192.168.214.203:5432', 'adb', 'plan_fact_202101', 'std9_121', 'password', 'login');

select * from std9_121.ch_plan_fact_ext where region = 'R002' and matdirec = '06' and distr_chan = '2';
select COUNT(*) from std9_121.ch_plan_fact_ext;


--3. Создание словарей на всех узлах кластера для доступа к данным таблиц системы Greenplum:
--         ch_price_dict
--         ch_chanel_dict
--         ch_product_dict
--         ch_region_dict

-- drop dictionary std9_121.ch_price_dict;
CREATE DICTIONARY IF NOT EXISTS std9_121.ch_price_dict ON CLUSTER default_cluster (
    `material` String,
    `region` String,
    `dist_chan` String,
    `price` Decimal(19,4)
)
PRIMARY KEY material
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std9_121' PASSWORD 'iDYWw8N8ZVLpLf' DB 'adb' TABLE 'std9_121.price'))
LIFETIME(0)
LAYOUT(hashed());

select * from std9_121.ch_price_dict;

-- drop dictionary std9_121.ch_chanel_dict;
CREATE DICTIONARY IF NOT EXISTS std9_121.ch_chanel_dict ON CLUSTER default_cluster (
    `dist_chan` String,
    `txtsh` String
)
PRIMARY KEY dist_chan
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std9_121' PASSWORD 'iDYWw8N8ZVLpLf' DB 'adb' TABLE 'std9_121.chanel'))
LIFETIME(0)
LAYOUT(hashed());

select * from std9_121.ch_chanel_dict;

-- drop dictionary std9_121.ch_region_dict;
CREATE DICTIONARY IF NOT EXISTS std9_121.ch_region_dict ON CLUSTER default_cluster (
    `region` String,
    `txt` String
)
PRIMARY KEY region
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std9_121' PASSWORD 'iDYWw8N8ZVLpLf' DB 'adb' TABLE 'std9_121.region'))
LIFETIME(0)
LAYOUT(complex_key_hashed());

select * from std9_121.ch_region_dict;

-- drop dictionary std9_121.ch_product_dict;
CREATE DICTIONARY IF NOT EXISTS std9_121.ch_product_dict ON CLUSTER default_cluster (
    `material` String,
    `asgrp` String,
    `brand` String,
    `matcateg` String,
    `matdirec` String,
    `txt` String  
)
PRIMARY KEY material
SOURCE(POSTGRESQL(PORT '5432' HOST '192.168.214.203' USER 'std9_121' PASSWORD 'iDYWw8N8ZVLpLf' DB 'adb' TABLE 'std9_121.product'))
LIFETIME(0)
LAYOUT(hashed());

select * from std9_121.ch_product_dict;

select * from system.dictionaries; 


--4. Создание реплицированных таблиц ch_plan_fact на всех хостах кластера.
-- DROP TABLE std9_121.ch_plan_fact;
CREATE TABLE IF NOT EXISTS std9_121.ch_plan_fact -- ON CLUSTER default_cluster
(
    `region` String,
    `matdirec` String,
    `distr_chan` String,
    `sales_qt` Int32,
    `plan_qt` Int32,
    `plan_exec_perc` Float32,
    `material` String
)
ENGINE = ReplicatedMergeTree('/click/std9_121.ch_plan_fact/{shard}', '{replica}')
ORDER BY region;

SELECT * FROM std9_121.ch_plan_fact;

SELECT * FROM system.clusters;
SELECT * FROM system.macros;


--5. Создание распределённой таблицы ch_plan_fact_distr, выбор для неё ключа шардирования.
--drop table std9_121.ch_plan_fact_distr;
CREATE TABLE std9_121.ch_plan_fact_distr AS std9_121.ch_plan_fact
ENGINE = Distributed('default_cluster', 'std9_121', 'ch_plan_fact', cityHash64(region, matdirec, distr_chan));

--Вставка всех записей из таблицы ch_plan_fact_ext.
INSERT INTO std9_121.ch_plan_fact_distr SELECT * FROM std9_121.ch_plan_fact_ext;  

SELECT * FROM std9_121.ch_plan_fact_distr;

SELECT * FROM std9_121.ch_plan_fact;













