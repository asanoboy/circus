create table an_page (
    page_id INT NOT NULL,
    name varchar(255) NOT NULL,
    infotype VARCHAR(255) binary NOT NULL,
    infocontent TEXT NOT NULL,
    contentlength INT NOT NULL,
    primary key (page_id)
);

create table an_info(
    text_id INT NOT NULL PRIMARY KEY,
    name varchar(255) binary NOT NULL,
    featured tinyint NOT NULL default 0,
    redirect_to INT NULL default NULL,
    index name(name)
);

CREATE TABLE `an_category_info` (
    `cat_id` int(11) NOT NULL,
    `infotype` varchar(255) binary NOT NULL,
    `page_num` int(11) NOT NULL,
    parent int null default null,
    featured tinyint not null default 0,
    PRIMARY KEY (`cat_id`,`infotype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


create table an_page_node_relation (
    page_id int NOT NULL,
    node_id int NOT NULL UNIQUE,
    PRIMARY KEY(page_id, node_id)
);

create table an_category_node_relation (
    cat_id int NOT NULL,
    node_id int NOT NULL UNIQUE,
    PRIMARY KEY(cat_id, node_id)
);

create table an_pagecount (
    page_id int NOT NULL,
    year int not null,
    count int not null,
    primary key(page_id, year)
);

create table an_catcount (
    cat_id int NOT NULL,
    year int not null,
    count int not null,
    primary key(cat_id, year)
);

create table an_feature_page (
    page_id int NOT NULL,
    target_infotype varchar(255) binary NOT NULL,
    primary key(page_id, target_infotype)
);

create table an_pagelinks (
    id_from int NOT NULL,
    id_to int NOT NULL,
    primary key(id_from, id_to),
    index id_to(id_to)
);

create table an_pagelinks_filtered (
    id_from int NOT NULL,
    id_to int NOT NULL,
    in_infobox tinyint NOT NULL,
    pos_info int NULL,
    pos_content int NOT NULL,
    primary key(id_from, id_to),
    index id_to(id_to)
);

create table an_pagelinks_featured (
    id_from int NOT NULL,
    id_to int NOT NULL,
    strength float NOT NULL DEFAULT 0,
    primary key(id_from, id_to),
    index id_to(id_to)
);

create view an_pagelinks_view as
select pl.*,
pfrom.name name_from, pfrom.infotype info_from,
pto.name name_to, pto.infotype info_to from an_pagelinks pl
inner join an_page pfrom on pfrom.page_id = pl.id_from
inner join an_page pto on pto.page_id = pl.id_to
;

create view an_pagelinks_filtered_view as
select pl.*,
pfrom.name name_from, pfrom.infotype info_from,
pto.name name_to, pto.infotype info_to from an_pagelinks_filtered pl
inner join an_page pfrom on pfrom.page_id = pl.id_from
inner join an_page pto on pto.page_id = pl.id_to
;

create view an_pagelinks_featured_view as
select pl.*,
pfrom.name name_from, pfrom.infotype info_from,
pto.name name_to, pto.infotype info_to from an_pagelinks_featured pl
inner join an_page pfrom on pfrom.page_id = pl.id_from
inner join an_page pto on pto.page_id = pl.id_to
;

create view page_view as
select * from page
where page_namespace = 0
;
