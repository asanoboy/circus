

class ItemFeatureBuilder:
    def __init__(self, master_db):
        self.db = master_db

    def build(self):
        self.db.updateQuery('''
        insert into feature_item(feature_id, item_id, strength)
        select fi.feature_id, fi.item_id, 0 strength
        from feature_item_lang fi
        inner join
        (
            select feature_id, lang
            from feature_item_lang
            group by feature_id, lang
            having count(*) > 3
        ) filtered on filtered.feature_id = fi.feature_id
            and filtered.lang = fi.lang
        group by fi.feature_id, fi.item_id
        ''')
        self.db.commit()
