
create table item (
    item_id int not null,
    visible tinyint not null,
    primary key(item_id)
);

create table item_page (
    item_id int not null,
    lang varchar(20) not null,
    page_id int not null,
    name varchar(255) not null, 
    view_count int not null,
    primary key(item_id, lang),
    index lang(lang, page_id)
);

create table feature_item_lang (
    feature_id int not null,
    item_id int not null,
    strength float not null,
    lang varchar(20) not null,
    index feature_id(feature_id),
    index lang(lang, item_id)
);

/* hard coding */
create table tag (
    tag_id int not null,
    name varchar(255) not null,
    primary key(tag_id)
);

create table tag_item (
    item_id int not null,
    tag_id int not null,
    primary key(item_id),
    index tag_id(tag_id)
);

/* hard coding */
create table feature_type (
    feature_type_id int not null,
    name varchar(255) not null
);

create table feature (
    feature_id int not null auto_increment,
    feature_type_id int not null,
    item_id int not null,
    primary key(feature_id),
    index feature_type_id(feature_type_id, item_id)
);

create table feature_item (
    feature_id int not null,
    item_id int not null,
    strength float not null,
    index feature_id(feature_id),
    index item_id(item_id)
);

create table feature_feature (
    id_from int not null,
    id_to int not null,
    strength float not null,
    primary key(id_from, id_to),
    index(id_to)
);

create view v_feature_item_lang_view as
select fil.*, ip2.name feature_name, ip.name item_name from feature_item_lang fil
inner join item_page ip on ip.item_id = fil.item_id and ip.lang = fil.lang
inner join feature f on f.feature_id = fil.feature_id
inner join item_page ip2 on ip2.item_id = f.item_id and ip2.lang = fil.lang
;

/*
create table category (
    cat_id int not null,
    primary key(cat_id)
);

create table info (
    info_id int not null,
    primary key(info_id)
);

create table category_lang_relation (
    cat_id int not null,
    lang varchar(20) not null,
    lang_cat_id int not null,
    primary key(lang_cat_id, lang),
    index(cat_id)
);

create table info_lang_relation (
    info_id int not null,
    lang varchar(20) not null,
    lang_info_id int not null,
    primary key(lang_info_id, lang),
    index(info_id)
);
*/
