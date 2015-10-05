create table page (
    id int NOT NULL primary key auto_increment,
    page_type tinyint NOT NULL,
    code varchar(64) NOT NULL,
    name varchar(256) NOT NULL,
    worksnum int NOT NULL,
    links text NOT NULL,
    top_contents text,
    index page_id(page_id),
    index id(page_type)
);

create table album (
    id int NOT NULL primary key auto_increment,
    code varchar(64) NOT NULL,
    name varchar(256) NOT NULL,
    page_id int NOT NULL,
    breadcrumb text NOT NULL,
    review_num int NOT NULL,
    index page_id(page_id)
);

create table pagelinks (
    id_from int NOT NULL,
    id_to int NOT NULL,
    odr float NOT NULL,
    index id_from(id_from),
    index id_to(id_to)
);

create view v_pagelinks as
select pl.*,
pfrom.name name_from,
pto.name name_to from pagelinks pl
inner join page pfrom on pfrom.id = pl.id_from
inner join page pto on pto.id = pl.id_to
;

/* fragments */
create table page_review_0 (
    page_id int not null primary key,
    num int not null
);

insert into page_review_0
select tmp.page_id, tmp.num
from (select p.id page_id, ( select review_num from album a where a.page_id=p.id order by review_num desc limit 1, 1 ) num
from page p) tmp
where tmp.num is not null;
