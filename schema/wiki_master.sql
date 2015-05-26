
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
    page_id int not null,
    lang varbinary(20) not null,
    lang_page_id int not null,
    name varchar(255) not null, 
    primary key(lang_page_id, lang),
    index(page_id)
);

create table category_lang_relation (
    cat_id int not null,
    lang varbinary(20) not null,
    lang_cat_id int not null,
    primary key(lang_cat_id, lang),
    index(cat_id)
);

create table info_lang_relation (
    info_id int not null,
    lang varbinary(20) not null,
    lang_info_id int not null,
    primary key(lang_info_id, lang),
    index(info_id)
);
