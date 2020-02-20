
-- create table

CREATE TABLE IF NOT EXISTS ods_haha (`id` BIGINT,`name` STRING) row format delimited fields terminated  BY '\t' stored as orc;


CREATE EXTERNAL TABLE IF NOT EXISTS haha (`id` BIGINT,`name` STRING) partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS textfile location '/tmp/database/hive/test/haha/';

CREATE EXTERNAL TABLE IF NOT EXISTS haha_insert (`id` BIGINT,`name` STRING) partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS textfile location '/tmp/database/hive/test/haha_insert/';

CREATE EXTERNAL TABLE IF NOT EXISTS haha_update (`id` BIGINT,`name` STRING) partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS textfile location '/tmp/database/hive/test/haha_update/';

CREATE EXTERNAL TABLE IF NOT EXISTS haha_delete (`id` BIGINT,`name` STRING) partitioned by (dt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS textfile location '/tmp/database/hive/test/haha_delete/';



-- load data

load data local inpath '/root/evan/haha' into table haha partition(dt='20200218');

load data local inpath '/root/evan/haha_insert' into table haha_insert partition(dt='20200219');

load data local inpath '/root/evan/haha_update' into table haha_update partition(dt='20200219');

load data local inpath '/root/evan/haha_delete' into table haha_delete partition(dt='20200219');




--- update 

insert overwrite table haha partition(dt='20200220') select  * from ( SELECT h.id,h.name FROM student.haha h LEFT JOIN (select id, name from student.haha_update where dt='20200220') hu on h.id = hu.id where hu.id is null and h.dt ='20200218' UNION
select id,name from haha_update where dt = '20200220')as aa;

-- delete

insert overwrite table haha partition(dt='20200220')
SELECT h.id,h.name FROM student.haha h LEFT JOIN (select id, name from student.haha_delete where dt='20200219') hu on h.id = hu.id
where hu.id is null and h.dt ='20200219'
 
-- insert

insert overwrite table haha partition(dt='20200220')
select  * from (
SELECT id,name FROM student.haha where h.dt ='20200219'
UNION
select id,name from haha_insert where dt = '20200219'
)as aa;



--load data to ods_haha
insert overwrite table ods_haha 
select  id,name from student.haha where h.dt ='20200220';
