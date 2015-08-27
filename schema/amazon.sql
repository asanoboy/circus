create table page (
    id int NOT NULL primary key auto_increment,
    page_type tinyint NOT NULL,
    page_id varchar(64) NOT NULL,
    name varchar(256) NOT NULL,
    worksnum int NOT NULL,
    links text NOT NULL,
    index page_id(page_id),
    index id(page_type)
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
