# configure replication user
create user if not exists 'replication_user'@'%' identified by 'secret123passwd';
grant replication client on *.* to 'replication_user'@'%';
grant replication slave on *.* to 'replication_user'@'%';
flush privileges;

use tap_mysql_test;

# create objects
create table r1 (
    i1 int auto_increment primary key,
    c1 varchar(100),
    d1 datetime default current_timestamp()
);

select * from r1;

insert into r1 (c1) values ('#1'),('#2'),('#3'),('#4'),('#5'),('#6'),('#7');
insert into r1 (c1) values ('#8'),('#9'),('#10'),('#11'),('#12'),('#13'),('#14');
insert into r1 (c1) values ('#15'),('#16'),('#17'),('#18');

update r1 set c1=concat(c1, '- updated 1') where i1 < 10;

create table r2 (
    i2 int primary key,
    d2 datetime
) ;
insert into r2 (i2, d2) values (1, now()), (2, now()), (3, now()), (4, now());

update r1 set c1=concat(c1, '- update 2') where i1 >= 10;

select * from r2;

delete from r1 where i1 < 4;

drop table r2;

alter table r1 add column b1 bool default False;
insert into r1 (c1, b1) values ('#8', True);

create table perf_test (
    id int auto_increment primary key,
    val varchar(100),
    ts datetime default current_timestamp(),
    tags json,
    meta json
);

insert into perf_test (val, tags, meta)
select
    concat('row-', a.n + b.n * 10 + c.n * 100 + d.n * 1000 + e.n * 10000 + f.n * 100000 + 1),
    json_array(concat('tag-', a.n), concat('tag-', b.n), concat('tag-', c.n)),
    json_object('a', a.n, 'b', b.n, 'label', concat('meta-', a.n, '-', b.n))
from (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) a
cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) b
cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) c
cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) d
cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) e
cross join (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4) f
where a.n + b.n * 10 + c.n * 100 + d.n * 1000 + e.n * 10000 + f.n * 100000 < 500000;
