create table page_ex (
    page_id INT NOT NULL,
    name varchar(255) NOT NULL,
    infotype VARCHAR(255) binary NOT NULL,
    infocontent TEXT NOT NULL,
    contentlength INT NOT NULL,
    primary key (page_id)
);

create table info_ex (
    text_id INT NOT NULL PRIMARY KEY,
    name varchar(255) binary NOT NULL,
    featured tinyint NOT NULL default 0,
    redirect_to INT NULL default NULL,
    index name(name)
);

CREATE TABLE `category_info` (
    `cat_id` int(11) NOT NULL,
    `infotype` varchar(255) binary NOT NULL,
    `page_num` int(11) NOT NULL,
    parent int null default null,
    featured tinyint not null default 0,
    PRIMARY KEY (`cat_id`,`infotype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table node (
    node_id int NOT NULL PRIMARY KEY,
    name varchar(255) binary NOT NULL
);

create table tree_node_relation (
    node_id int NOT NULL,
    parent_id int NULL,
    all_child_num int NOT NULL,
    treetype varchar(255) binary NOT NULL,
    PRIMARY KEY(node_id, treetype)
);

create table feature_node_relation (
    feature_node_id int NOT NULL,
    node_id int NOT NULL,
    weight float NOT NULL,
    PRIMARY KEY(feature_node_id),
    INDEX node_to(node_id)
);

create table page_node_relation (
    page_id int NOT NULL,
    node_id int NOT NULL UNIQUE,
    PRIMARY KEY(page_id, node_id)
);

create table category_node_relation (
    cat_id int NOT NULL,
    node_id int NOT NULL UNIQUE,
    PRIMARY KEY(cat_id, node_id)
);
