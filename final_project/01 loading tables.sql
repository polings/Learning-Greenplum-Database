-- 1.1 Заполнение таблиц справочников
select std9_121.f_full_load('std9_121.coupons_ext', 'std9_121.coupons', true);
select std9_121.f_full_load('std9_121.stores_ext', 'std9_121.stores', true);
select std9_121.f_full_load('std9_121.promos_ext', 'std9_121.promos', true);
select std9_121.f_full_load('std9_121.promo_types_ext', 'std9_121.promo_types', true);

-- 1.2 Загрузка таблиц фактов
select std9_121.f_full_load('std9_121.traffic_ext', 'std9_121.traffic', true);
select std9_121.f_load_delta_partitions('std9_121.bills_item_ext', 'std9_121.bills_item', 'calday', '2021-01-01', '2021-02-28');
select std9_121.f_load_delta_partitions('std9_121.bills_head_ext', 'std9_121.bills_head', 'calday', '2021-01-01', '2021-02-28');


-- 2 Загрузка таблиц с изменением типа хранения полей
select std9_121.f_full_load_coupons('std9_121.coupons', 'std9_121.ods_coupons', true);
select std9_121.f_full_load_promos('std9_121.promos', 'std9_121.ods_promos', true);
select std9_121.f_load_delta_partitions_traffic('std9_121.traffic', 'std9_121.ods_traffic', '2021-01-01', '2021-02-28');


-- 3. Загрузка таблиц в детальный слой для использования в витринах данных
select std9_121.f_full_load_dds_coupons('std9_121.ods_coupons', 'std9_121.dds_coupons', true);
select std9_121.f_load_delta_partitions_dds_bills('std9_121.bills_item', 'std9_121.dds_bills', 'bi.calday', '2021-01-01', '2021-02-28');
