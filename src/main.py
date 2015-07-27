import argparse
from config import dbhost, user, log_dir, pw
from dbutils import WikiDB, open_session
from model.master import Base
from debug import get_logger, set_config
from builders.pagelinks_filtered import PagelinksFilteredBuilder
from builders.pagelinks_featured import PagelinksFeaturedBuilder
from builders.pagelinks import PagelinksBuilder
from builders.pagelinks_multi import Builder as PagelinksMultiBuilder
from builders.item_pagelinks import Builder as ItemPagelinksBuilder
from builders.pagelinks_music_genre import Builder as PagelinksMusicGenreBuilder
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
        logger = get_logger(__name__)
        for b in self.builders:
            with logger.lap('%s, %s' % (self.name, b.__class__.__name__)):
                b.build()


if __name__ == '__main__':
    set_config(log_dir + '/log')
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--langs', required=True)  # ja,en
    parser.add_argument('-t', '--trunc', action='store_true')
    args = parser.parse_args()
    args = vars(args)

    langs = args['langs'].split(',')
    truncate = args['trunc']
    imported_langs = ['en', 'ja']

    # master_db = MasterWikiDB('wikimaster')
    lang_to_db = { l: WikiDB(l, user, dbhost, pw) for l in imported_langs }
    with open_session(dbhost, user, 'master', Base, pw, truncate=truncate) as session:
        for lang in langs:
            wiki_db = lang_to_db[lang]
            other_dbs = [db for db in lang_to_db.values() if db.lang != lang]

            holder = BuilderHolder(lang)
            #holder.push(InfoBuilder(wiki_db))
            #holder.push(InfoboxBuilder(wiki_db))
            #holder.push(PageBuilder(wiki_db))
            #holder.push(CategoryBuilder(wiki_db))
            #holder.push(PagelinksBuilder(wiki_db))
            #holder.push(PagelinksMultiBuilder(wiki_db))
            #holder.push(PagelinksFilteredBuilder(wiki_db))
            #holder.push(PagelinksFeaturedBuilder(wiki_db))

            ## holder.push(ItemTagBuilder(master_db, wiki_db, other_dbs))
            # holder.push(FeatureBuilder(session, wiki_db, other_dbs))
            # holder.push(ItemPagelinksBuilder(session, wiki_db))
            holder.push(PagelinksMusicGenreBuilder(session, wiki_db))
            holder.build()

        holder = BuilderHolder('master')
        ## holder.push(PopularityCalc(session, lang_to_db.values()))
        # holder.push(ItemFeatureBuilder(session))
        # holder.push(FeatureRelationBuilder(session))
        # holder.push(StrengthCalc(master_db))
        holder.build()
