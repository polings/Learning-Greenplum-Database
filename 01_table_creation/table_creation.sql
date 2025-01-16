-- Создание таблиц справочников
CREATE TABLE std9_121.price ( 
	material varchar(20) NOT NULL,
	region varchar(20) NOT NULL,
	dist_chan varchar(100) NOT NULL,
	price numeric(19, 4)
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.product ( 
	material varchar(20) NOT NULL,
	asgrp varchar(20) NULL,
	brand varchar(20) NULL,
	matcateg char NULL,
	matdirec varchar(10) NULL,
	txt varchar(100) NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.chanel ( 
	dist_chan varchar(100) NOT NULL,
	txtsh varchar(100) NULL
) DISTRIBUTED REPLICATED;

CREATE TABLE std9_121.region ( 
	region varchar(20) NOT NULL,
	txt varchar(50) NULL
) DISTRIBUTED REPLICATED;

-- Создание таблицы фактов
CREATE TABLE std9_121.sales ( 
	check_nm varchar(100) NOT NULL,
	check_pos varchar(100) NOT NULL,
	region varchar(20) NULL,
	material varchar(20) NULL,
	distr_chan varchar(100) NULL,
	quantity int4 NULL,
	"date" date NULL
)
WITH (
	appendoptimized=true,
	orientation=column,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED BY (check_nm) 
PARTITION BY RANGE ("date")
(
	start ("date" '2021-01-01') inclusive
	end ("date" '2022-01-01') exclusive
	every (interval '1 month'),
	default partition def
);


CREATE TABLE std9_121.plan ( 
	"date" date NOT NULL,
	region varchar(20) NOT NULL,
	matdirec varchar(20) NOT NULL,
	quantity int4 NULL,
	distr_chan varchar(100) NOT NULL
)
WITH (
	appendoptimized=true,
	orientation=row,
	compresstype=zstd,
	compresslevel=1
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE ("date")
(
	start ("date" '2021-01-01') inclusive
	end ("date" '2022-01-01') exclusive
	every (interval '1 month'),
	default partition def
);
