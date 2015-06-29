from model.master import Feature, Item, FeatureItemAssoc


class ItemFeatureBuilder:
    def __init__(self, master):
        self.master = master

    def build(self):
        assoc_list = []
        item_id_to_assoc = {}
        for f in self.master.query(Feature):
            item_num = len(f.items)
            for i in f.items:
                assoc = FeatureItemAssoc(
                        feature_id=f.id,
                        item_id=i.id,
                        strength=100/item_num)
                assoc_list.append(assoc)
                if i.id not in item_id_to_assoc:
                    item_id_to_assoc[i.id] = []
                item_id_to_assoc[i.id].append(assoc)
        
        for item_id, assocs_by_item in item_id_to_assoc.items():
            feature_num = len(assocs_by_item)
            for assoc in assocs_by_item:
                assoc.strength *= 100/feature_num

        self.master.add_all(assoc_list)
        self.master.flush()

