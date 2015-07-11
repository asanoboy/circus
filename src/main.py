import argparse
from config import dbhost, user
from dbutils import MasterWikiDB, WikiDB, open_session
from model.master import Base
# from numerical import *
from debug import Lap
from builders.pagelinks_filtered import PagelinksFilteredBuilder
from builders.pagelinks_featured import PagelinksFeaturedBuilder
from builders.pagelinks import PagelinksBuilder
from builders.info_builder import InfoBuilder
from builders.page_builder import PageBuilder
from builders.category_builder import CategoryBuilder
from builders.itemtag_builder import ItemTagBuilder
from builders.feature_builder import FeatureBuilder
from builders.item_feature_builder import ItemFeatureBuilder
from builders.popularity_calc import PopularityCalc
from builders.feature_relation_builder2 import FeatureRelationBuilder
from builders.strength_calc import StrengthCalc
from builders.infobox import Builder as InfoboxBuilder


class BuilderHolder:
    def __init__(self, name):
        self.builders = []
        self.name = name

    def push(self, builder):
        self.builders.append(builder)

    def build(self):
        for b in self.builders:
            with Lap('%s, %s' % (self.name, b.__class__.__name__)):
                b.build()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--langs')  # ja,en
    args = parser.parse_args()
    args = vars(args)

    langs = args['langs'].split(',')
    imported_langs = ['en', 'ja']

    # master_db = MasterWikiDB('wikimaster')
    lang_to_db = { l: WikiDB(l) for l in imported_langs }
    with open_session(dbhost, user, 'master', Base, truncate=True) as session:
        for lang in langs:
            wiki_db = lang_to_db[lang]
            other_dbs = [db for db in lang_to_db.values() if db.lang != lang]

            holder = BuilderHolder(lang)
            #holder.push(InfoBuilder(wiki_db))
            #holder.push(InfoboxBuilder(wiki_db))
            #holder.push(PageBuilder(wiki_db))
            #holder.push(CategoryBuilder(wiki_db))
            #holder.push(PagelinksBuilder(wiki_db))
            #holder.push(PagelinksFilteredBuilder(wiki_db))
            #holder.push(PagelinksFeaturedBuilder(wiki_db))

            ## holder.push(ItemTagBuilder(master_db, wiki_db, other_dbs))
            holder.push(FeatureBuilder(session, wiki_db, other_dbs))
            holder.build()

        holder = BuilderHolder('master')
        ## holder.push(PopularityCalc(session, lang_to_db.values()))
        # holder.push(ItemFeatureBuilder(session))
        # holder.push(FeatureRelationBuilder(session))
        #h older.push(StrengthCalc(master_db))
        holder.build()
