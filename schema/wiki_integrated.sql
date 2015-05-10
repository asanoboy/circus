


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
    PRIMARY KEY(feature_node_id, node_id),
    INDEX node_to(node_id)
);
