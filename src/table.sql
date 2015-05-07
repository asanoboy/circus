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
    featured tinyint not null default 0,
    PRIMARY KEY (`cat_id`,`infotype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

