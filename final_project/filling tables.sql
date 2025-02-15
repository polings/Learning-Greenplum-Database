-- 1.1 Заполнение таблиц справочников
select std9_121.f_full_load(p_table_from := 'std9_121.src_coupons_ext',
							p_table_to := 'std9_121.src_coupons',
							p_truncate_tgt := true);

select std9_121.f_full_load(p_table_from := 'std9_121.stores_ext',
							p_table_to := 'std9_121.stores',
							p_truncate_tgt := true);

select std9_121.f_full_load(p_table_from := 'std9_121.promos_ext',
							p_table_to := 'std9_121.promos',
							p_truncate_tgt := true);

select std9_121.f_full_load(p_table_from := 'std9_121.promo_types_ext',
							p_table_to := 'std9_121.promo_types',
							p_truncate_tgt := true);

-- 1.2 Загрузка справочника с изменением типа хранения для поля дата
select std9_121.f_full_load_coupons(p_table_from := 'std9_121.src_coupons',
							        p_table_to := 'std9_121.coupons',
							        p_truncate_tgt := true);

-- 2.1 Загрузка таблиц фактов
select std9_121.f_load_delta_partitions('std9_121.bills_item_ext', 'std9_121.bills_item', 'calday', '2021-01-01', '2021-02-28');
select std9_121.f_load_delta_partitions('std9_121.bills_head_ext', 'std9_121.bills_head', 'calday', '2021-01-01', '2021-02-28');
-- select std9_121.f_load_delta_partitions('std9_121.src_traffic_ext', 'std9_121.src_traffic', 'calday', '2021-01-01', '2021-02-28');


-- 2.2 Загрузка таблицы traffic с изменением типа хранения для поля дата
select std9_121.f_full_load('std9_121.src_traffic_ext', 'std9_121.src_traffic',true);
select std9_121.f_load_delta_partitions_traffic('std9_121.src_traffic', 'std9_121.traffic', '2021-01-01', '2021-02-28');
