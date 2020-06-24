drop table Colors;

create table Colors (
 red smallint,
 green smallint,
 blue smallint)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:P}' overwrite into table Colors;

select 1,red,count(1) as r from Colors group by Colors.red;
select 2,green,count(1) as g from Colors group by Colors.green;
select 3,blue,count(1) as b from Colors group by Colors.blue;