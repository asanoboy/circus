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
