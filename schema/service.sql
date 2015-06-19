


create table session (
    session_id int not null auto_increment,
    seed int not null,
    lang varchar(20) not null,
    created_at datetime not null default current_timestamp,
    primary key(session_id)
);

create table access (
    access_id int not null auto_increment,
    session_id int not null,
    item_id int not null,
    choice tinyint not null,
    created_at datetime not null default current_timestamp,
    primary key(access_id),
    index session_id(session_id)
);
