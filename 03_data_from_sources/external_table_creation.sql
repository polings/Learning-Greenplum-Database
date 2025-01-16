-- Creating external table with pxf
create external table std9_121.sales_ext (like std9_121.sales)
location ('pxf://gp.sales?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) on all 
format 'CUSTOM' (FORMATTER='pxfwritable_import')
encoding 'UTF8';

create external table std9_121.plan_ext (like std9_121.plan)
location ('pxf://gp.plan?PROFILE=jdbc&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
) on all 
format 'CUSTOM' (FORMATTER='pxfwritable_import')
encoding 'UTF8';


-- Creating external table with gpfdist
-- Should be connected in the console: gpfdist -d path\to\external\files -p 8080
create external table std9_121.ext_price (like std9_121.price)
location ('gpfdist://172.16.128.22:8080/price.csv'
) on all
format 'CSV' (delimiter ';' null '' quote '"')
encoding 'UTF8'
segment reject limit 10 rows;

create external table std9_121.ext_chanel (like std9_121.chanel)
location ('gpfdist://172.16.128.22:8080/chanel.csv'
) on all
format 'CSV' (delimiter ';' null '' quote '"')
encoding 'UTF8'
segment reject limit 10 rows;

create external table std9_121.ext_product (like std9_121.product)
location ('gpfdist://172.16.128.22:8080/product.csv'
) on all
format 'CSV' (delimiter ';' null '' quote '"')
encoding 'UTF8'
segment reject limit 10 rows;

create external table std9_121.ext_region (like std9_121.region)
location ('gpfdist://172.16.128.22:8080/region.csv'
) on all
format 'CSV' (delimiter ';' null '' quote '"' HEADER)
encoding 'UTF8'
segment reject limit 10 rows;
