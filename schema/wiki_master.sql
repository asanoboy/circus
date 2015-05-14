
create table page (
    page_id int not null,
    primary key(page_id)
);

create table category (
    cat_id int not null,
    primary key(cat_id)
);

create table info (
    info_id int not null,
    primary key(info_id)
);

create table page_lang_relation (
    page_id int not null auto_increment,
    lang varbinary(20) not null,
    lang_page_id int not null,
    primary key(page_id),
    unique index(lang_page_id, lang)
);

create table category_lang_relation (
    cat_id int not null auto_increment,
    lang varbinary(20) not null,
    lang_cat_id int not null,
    primary key(cat_id),
    unique index(lang_cat_id, lang)
);

create table info_lang_relation (
    info_id int not null auto_increment,
    lang varbinary(20) not null,
    lang_info_id int not null,
    primary key(info_id),
    unique index(lang_info_id, lang)
);
