-- 1.1 Создание таблиц фактов
CREATE TABLE std9_121.traffic (
    plant    bpchar(4)  NULL,
    "date"   bpchar(10) NULL,
    "time"   bpchar(6)  NULL,
    frame_id bpchar(10) NULL,
    quantity int4       NULL
)
    WITH (
        appendoptimized = true,
        orientation = row, -- Так как загрузка данных раз в час
        compresstype = zstd,
        compresslevel = 1
        )
    DISTRIBUTED RANDOMLY;

CREATE TABLE std9_121.ods_traffic (
    plant    bpchar(4)  NULL,
    calday   date       NULL,
    "time"   bpchar(6)  NULL,
    frame_id bpchar(10) NULL,
    quantity int4       NULL
)
    WITH (
        appendoptimized = true,
        orientation = row,
        compresstype = zstd,
        compresslevel = 1
        )
    DISTRIBUTED RANDOMLY
    PARTITION BY RANGE (calday) (
        START ('2021-01-01') INCLUSIVE
            END ('2021-02-01') EXCLUSIVE
            EVERY (INTERVAL '1 month'),
        DEFAULT PARTITION def
        );

CREATE TABLE std9_121.bills_head (
    billnum int8      NULL,
    plant   bpchar(4) NULL,
    calday  date      NULL
)
    WITH (
        appendoptimized = true,
        orientation = column, -- Скорее всего загрузка большого кол-ва строк раз в день (не сильно часто)
        compresstype = zstd,
        compresslevel = 1
        )
    DISTRIBUTED BY (billnum)
    PARTITION BY RANGE (calday) (
        START ('2021-01-01') INCLUSIVE
            END ('2021-02-01') EXCLUSIVE
            EVERY (INTERVAL '1 month'),
        DEFAULT PARTITION def
        );

CREATE TABLE std9_121.bills_item (
    billnum  int8           NULL,
    billitem int8           NULL,
    material int8           NULL,
    qty      int8           NULL,
    netval   numeric(17, 2) NULL,
    tax      numeric(17, 2) NULL,
    rpa_sat  numeric(17, 2) NULL,
    calday   date           NULL
)
    WITH (
        appendoptimized = true,
        orientation = column,
        compresstype = zstd,
        compresslevel = 1
        )
    DISTRIBUTED BY (billnum)
    PARTITION BY RANGE (calday) (
        START ('2021-01-01') INCLUSIVE
            END ('2021-02-01') EXCLUSIVE
            EVERY (INTERVAL '1 month'),
        DEFAULT PARTITION def
        );

CREATE TABLE std9_121.dds_bills (
    billnum  int8           NULL,
    billitem int8           NULL,
    material int8           NULL,
    plant    bpchar(4)      NULL,
    qty      int8           NULL,
    netval   numeric(17, 2) NULL,
    tax      numeric(17, 2) NULL,
    rpa_sat  numeric(17, 2) NULL,
    calday   date           NULL
)
    WITH (
        appendoptimized = true,
        orientation = column,
        compresstype = zstd,
        compresslevel = 1
        )
    DISTRIBUTED BY (billnum)
    PARTITION BY RANGE (calday) (
        START ('2021-01-01') INCLUSIVE
            END ('2021-02-01') EXCLUSIVE
            EVERY (INTERVAL '1 month'),
        DEFAULT PARTITION def
        );


-- 1.2 Создание таблиц справочников
CREATE TABLE std9_121.coupons (
    plant     bpchar(4)  NULL,
    calday    bpchar(10) NULL,
    coupon_nm bpchar(7)  NULL,
    promo_id  bpchar(40) NULL,
    material  bpchar(15) NULL,
    billnum   bpchar(15) NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.ods_coupons (
    plant     bpchar(4)  NULL,
    calday    date       NULL,
    coupon_nm bpchar(7)  NULL,
    promo_id  bpchar(40) NULL,
    material  bigint     NULL,
    billnum   bigint     NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.dds_coupons (
    plant      bpchar(4)      NULL,
    calday     date           NULL,
    coupon_nm  bpchar(7)      NULL,
    promo_id   bpchar(40)     NULL,
    material   bigint         NULL,
    billnum    bigint         NULL,
    unit_price numeric(17, 2) NULL,
    promo_type bpchar(3)      NULL,
    discount   numeric(17, 2) NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.stores (
    plant bpchar(4)  NULL,
    txt   bpchar(20) NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.promos (
    promo_id   bpchar(40)     NULL,
    txt        bpchar(20)     NULL,
    promo_type bpchar(3)      NULL,
    material   bpchar(15)     NULL,
    discount   numeric(17, 2) NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.ods_promos (
    promo_id   bpchar(40)     NULL,
    txt        bpchar(20)     NULL,
    promo_type bpchar(3)      NULL,
    material   bigint         NULL,
    discount   numeric(17, 2) NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.promo_types (
    promo_type bpchar(3)  NULL,
    txt        bpchar(50) NULL
) DISTRIBUTED REPLICATED;


-- 2.1 Создание внешних таблиц pxf
SELECT std9_121.f_create_pxf_table('traffic', 'gp.traffic', 'intern', 'intern');
SELECT std9_121.f_create_pxf_table('bills_head', 'gp.bills_head', 'intern', 'intern');
SELECT std9_121.f_create_pxf_table('bills_item', 'gp.bills_item', 'intern', 'intern');


-- 2.1 Создание внешних таблиц gpfdist
--     gpfdist -d path\to\external\files -p 8080
--     gpfdist -d C:\Users\shpol\OneDrive\Documents\gpfdist -p 8080
SELECT std9_121.f_create_gpfdist_table('std9_121.coupons', 'coupons', '''CSV'' (HEADER DELIMITER '';'' NULL '''' QUOTE ''"'')');
SELECT std9_121.f_create_gpfdist_table('std9_121.stores', 'stores', '''CSV'' (HEADER DELIMITER '';'' NULL '''' QUOTE ''"'')');
SELECT std9_121.f_create_gpfdist_table('std9_121.promos', 'promos', '''CSV'' (HEADER DELIMITER '';'' NULL '''' QUOTE ''"'')');
SELECT std9_121.f_create_gpfdist_table('std9_121.promo_types', 'promo_types', '''CSV'' (HEADER DELIMITER '';'' NULL '''' QUOTE ''"'')');