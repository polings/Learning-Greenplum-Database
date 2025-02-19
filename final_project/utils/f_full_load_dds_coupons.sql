-- drop function std9_121.f_full_load_dds_coupons(p_table_from text, p_table_to text, p_truncate_tgt bool);
create or replace function std9_121.f_full_load_dds_coupons(p_table_from text, p_table_to text, p_truncate_tgt bool DEFAULT FALSE)
	returns int8
	language plpgsql
	security definer
	volatile
as $$
DECLARE
	v_location 	 text := 'std9_121.f_full_load_dds_coupons';
	v_table_from text;
	v_table_to 	 text;
	v_where 	 text;
    v_sql        text;
	v_cnt 		 int8;
BEGIN
	-- Unify names
	v_table_from = std9_121.f_unify_name(p_name := p_table_from);
	v_table_to = std9_121.f_unify_name(p_name := p_table_to);

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
			                     p_log_message := 'START Switch table '||v_table_to||' with table '||v_table_from,
			                     p_location    := v_location);

	-- Checking if source table is empty
	EXECUTE 'SELECT COUNT(1) FROM (SELECT * FROM '||v_table_from||' LIMIT 1) cnt' into v_cnt;
	IF v_cnt = 0 THEN
		-- Write end log
	    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
                                     p_log_message := 'END Switch table '||v_table_to||' with table '||v_table_from||', source table is empty',
                                     p_location    := v_location);
		RETURN 0;
	END IF;

	-- Truncate target table if needed
	IF p_truncate_tgt IS TRUE THEN
		PERFORM std9_121.f_truncate_table(v_table_to);
	END IF;

	-- Generate the condition
	v_where = COALESCE(std9_121.f_get_where_not_empty(v_table_to), ' 1 = 1 ');

	-- Write start log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
			                     p_log_message := 'START Insert data from table '||v_table_from||' to '||v_table_to || ' with condition: '||v_where,
			                     p_location    := v_location);
	-- Insert data
	v_sql := 'INSERT INTO ' || v_table_to || ' ' ||
             'SELECT DISTINCT oc.plant,
                       oc.calday,
                       oc.coupon_nm,
                       oc.promo_id,
                       oc.material,
                       oc.billnum,
                       bi.rpa_sat / bi.qty AS unit_price,
                       op.promo_type,
                       CASE WHEN op.promo_type = ''001'' THEN op.discount
                            WHEN op.promo_type = ''002'' THEN op.discount / 100 * (bi.rpa_sat / bi.qty)
                       END AS discount
             FROM '|| v_table_from ||' oc
             JOIN std9_121.bills_item bi ON bi.material = oc.material AND bi.billnum = oc.billnum
             JOIN std9_121.ods_promos op ON op.promo_id = oc.promo_id;';
	EXECUTE v_sql;

	GET DIAGNOSTICS v_cnt = ROW_COUNT;
	RAISE NOTICE '% rows inserted from % into %', v_cnt, v_table_from, v_table_to;

	-- Write end log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
	                         	 p_log_message := 'END Insert data from table '||v_table_from||' to '||v_table_to||', '||v_cnt||' rows inserted',
	                          	 p_location    := v_location);

	-- Analyze the table
	EXECUTE std9_121.f_analyze_table(v_table_to);

	-- Write end log
    PERFORM std9_121.f_write_log(p_log_type    := 'INFO',
			                     p_log_message := 'END Switch table '||v_table_to||' with table '||v_table_from,
			                     p_location    := v_location);
	RETURN v_cnt;
END;
$$
execute on any;

-- select std9_121.f_full_load_dds_coupons(p_table_from := 'std9_121.ods_coupons',
--                                         p_table_to := 'std9_121.dds_coupons',
--                                         p_truncate_tgt := true);
--
--
-- 'INSERT INTO ' || v_table_to || ' '
-- 'SELECT plant, calday, coupon_nm, promo_id, material, billnum, bi.rpa_sat / bi.qty AS unit_price, promo_type, discount FROM ' || v_table_from || ' oc'
-- 'JOIN std9_121.bills_item bi ON bi.material = oc.material AND bi.billnum = oc.billnum ' ||
-- 'WHERE '||v_where;
--
-- 'INSERT INTO ' || v_table_to || ' ' ||
-- 'SELECT DISTINCT oc.plant,
--        oc.calday,
--        oc.coupon_nm,
--        oc.promo_id,
--        oc.material,
--        oc.billnum,
--        bi.rpa_sat / bi.qty AS unit_price,
--        op.promo_type,
--        CASE WHEN op.promo_type = ''001'' THEN op.discount
--             WHEN op.promo_type = ''002'' THEN op.discount / 100 * (bi.rpa_sat / bi.qty)
--        END AS discount
-- FROM '|| v_table_from ||' oc
-- JOIN std9_121.bills_item bi ON bi.material = oc.material AND bi.billnum = oc.billnum
-- JOIN std9_121.ods_promos op ON op.promo_id = oc.promo_id;'
--
--
-- WITH cte AS (SELECT DISTINCT oc.plant,
--        oc.calday,
--        oc.coupon_nm,
--        oc.promo_id,
--        oc.material,
--        oc.billnum,
--        bi.rpa_sat / bi.qty AS unit_price,
--        op.promo_type,
--        CASE WHEN op.promo_type = '001' THEN op.discount
--             WHEN op.promo_type = '002' THEN op.discount / 100 * (bi.rpa_sat / bi.qty)
--        END AS discount
-- FROM std9_121.ods_coupons oc
-- JOIN std9_121.bills_item bi ON bi.material = oc.material AND bi.billnum = oc.billnum
-- JOIN std9_121.ods_promos op ON op.promo_id = oc.promo_id)
-- SELECT plant, sum(discount)
-- from cte
-- group by plant










