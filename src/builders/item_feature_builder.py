from model.master import Feature, FeatureItemAssoc


class ItemFeatureBuilder:
    def __init__(self, master):
        self.master = master

    def build(self):
        assoc_list = []
        for f in self.master.query(Feature):
            for i in f.items:
                assoc_list.append(
                    FeatureItemAssoc(
                        feature_id=f.id,
                        item_id=i.id,
                        strength=0))
        
        self.master.add_all(assoc_list)
        self.master.flush()
