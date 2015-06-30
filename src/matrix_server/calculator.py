from numerical import RelationMatrix
from debug import get_logger
from model.master import FeatureItemAssoc, FeatureRelationAssoc


use_ff = True


class Calculator:
    def __init__(self):
        self.logger = get_logger(__name__)

    def load(self, session):
        item_feature_mtx = RelationMatrix()
        lap = self.logger.lap
        with lap('set data'):
            for fi in session.query(FeatureItemAssoc):
                item_feature_mtx.append(
                    fi.item_id,
                    fi.feature_id,
                    fi.strength)

            feature_feature_mtx = RelationMatrix(
                src=item_feature_mtx.get_dst(),
                dst=item_feature_mtx.get_dst())

            if use_ff:
                for ff in session.query(FeatureRelationAssoc):
                    feature_feature_mtx.append(
                        ff.id_from,
                        ff.id_to,
                        ff.strength)

        with lap('build item_feature'):
            item_feature_mtx.build()

        with lap('build feature_item'):
            feature_item_mtx = item_feature_mtx.create_inverse()

        if use_ff:
            with lap('build feature_feature'):
                feature_feature_mtx.build()

        self.item_feature_mtx = item_feature_mtx
        self.feature_feature_mtx = feature_feature_mtx
        self.feature_item_mtx = feature_item_mtx

    def get_features_by_items(self, item_dict):
        with self.logger.lap('item_feature'):
            feature_dict = self.item_feature_mtx * item_dict
        return feature_dict

    def get_features_by_features(self, orig_feature_dict):
        if use_ff:
            with self.logger.lap('item_feature'):
                feature_dict = self.feature_feature_mtx * orig_feature_dict
            return feature_dict
        else:
            return dict()

    def get_items_by_features(self, feature_dict):
        with self.logger.lap('feature_item'):
            item_dict = self.feature_item_mtx * feature_dict
        return item_dict
