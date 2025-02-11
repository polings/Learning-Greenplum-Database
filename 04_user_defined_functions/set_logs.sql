-- Working with logs

-- Table to store logs
-- drop table std9_121.logs;
CREATE TABLE std9_121.logs (
	log_id int8 NOT NULL,
	log_timestamp timestamp NOT NULL DEFAULT now(),
	log_type text NOT NULL,
	log_msg text NOT NULL,
	log_location text NULL,
	is_error bool NULL,
	log_user text NULL DEFAULT "current_user"(),
	CONSTRAINT pk_log_id PRIMARY KEY (log_id)
)
	DISTRIBUTED BY (log_id);

-- Sequence to set log_id
create sequence std9_121.log_id_seq
	increment by 1
	minvalue 1
	maxvalue 9223372036854775807
	start 1;

-- Function to add log into table
-- DROP FUNCTION std9_121.f_write_log(text, text, text);
CREATE OR REPLACE FUNCTION std9_121.f_write_log(p_log_type text, p_log_message text, p_location text)
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$
DECLARE
	v_log_type text;
	v_log_message text;
	v_sql text;
	v_location text;
	v_res text;
BEGIN
    -- Check message type
    v_log_type = upper(p_log_type);
    v_location = lower(p_location);

    -- Only 'ERROR' and 'INFO' logs are allowable
    IF v_log_type NOT IN ('ERROR', 'INFO') THEN
        RAISE EXCEPTION 'Illegal log type! Use one of: ERROR, INFO';
    END IF;

    RAISE NOTICE '%: %: <%> Location [%]', clock_timestamp(), v_log_type, p_log_message, v_location;

    v_log_message = replace(p_log_message, '''', '''''');

    -- Write in log table
    v_sql := format(
            'INSERT INTO std9_121.logs(log_id, log_type, log_msg, log_location, is_error, log_timestamp, log_user)
             VALUES (nextval(''std9_121.log_id_seq''), %L, %L, %L, %s, current_timestamp, current_user);',
            v_log_type,
            coalesce(v_log_message, 'empty'),
            coalesce(v_location, 'null'),
            CASE WHEN v_log_type = 'ERROR' THEN 'TRUE' ELSE 'FALSE' END
        );

    RAISE NOTICE 'INSERT SQL IS: %', v_sql;

    -- Execution on different server to write record despite transaction rollback
    v_res= dblink('adb_server', v_sql);
END;
$$
EXECUTE ON ANY;