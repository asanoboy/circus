create view wk_v_feature_item_page as
select f.id feature_id, p_f.name feature_name, f.year, f.ref_item_id, p_f.lang feature_lang,
p_i.item_id, p_i.id page_id, p_i.name item_name, p_i.lang item_lang, p_i.popularity, p_i.linknum from feature_item_table fi
inner join feature f on f.id = fi.feature_id
inner join page p_f on p_f.item_id = f.ref_item_id
inner join page p_i on p_i.item_id = fi.item_id;
