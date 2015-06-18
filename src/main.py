import argparse
from dbutils import MasterWikiDB, WikiDB
from models import createPageInfoByBracketText, Category
# from numerical import *
from parser import getBracketTexts
from debug import Lap
from builders.pagelinks_filtered import PagelinksFilteredBuilder
from builders.pagelinks_featured import PagelinksFeaturedBuilder
from builders.pagelinks import PagelinksBuilder
from builders.page_builder import PageBuilder
from builders.itemtag_builder import ItemTagBuilder
from builders.feature_builder import FeatureBuilder
from builders.item_feature_builder import ItemFeatureBuilder
from builders.popularity_calc import PopularityCalc
from builders.feature_relation_builder import FeatureRelationBuilder


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


def selectTextByTitle(wiki_db, title, namespace):
    res = wiki_db.selectAndFetchAll("""
        select t.old_text wiki, p.page_id id from page p
        inner join revision r on r.rev_page = p.page_id
        inner join text t on t.old_id = r.rev_text_id
        where p.page_title = %s and p.page_namespace = %s
        """), (title, namespace)
    if len(res) > 0:
        return res[0]['wiki'].decode('utf-8')
    return False


def createCategoryWithoutStub(data):
    id = data[0]
    title = data[1]
    text = data[2]
    bracketTexts = getBracketTexts(text)
    infos = [createPageInfoByBracketText(t) for t in bracketTexts]
    infos = [i for i in infos if i]
    if len(infos) > 0 and infos[0].name.lower().find('stub') != -1:
        return False
    elif text.find('__HIDDENCAT__') != -1 or \
            text.lower().find('hiddencat}}') != -1:
        return False
    else:
        return Category(id, title)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--langs')  # ja,en
    args = parser.parse_args()
    args = vars(args)

    langs = args['langs'].split(',')
    imported_langs = ['en', 'ja']

    master_db = MasterWikiDB('wikimaster')
    lang_dbs = [WikiDB(l) for l in imported_langs]
    for lang in langs:
        wiki_db = WikiDB(lang)
        other_dbs = [db for db in lang_dbs if db.lang != lang]

        holder = BuilderHolder(lang)
        #holder.push(PageBuilder(wiki_db))
        #holder.push(PagelinksBuilder(wiki_db))
        #holder.push(PagelinksFilteredBuilder(wiki_db))
        #holder.push(PagelinksFeaturedBuilder(wiki_db))

        #holder.push(ItemTagBuilder(master_db, wiki_db, other_dbs))
        #holder.push(FeatureBuilder(master_db, wiki_db, other_dbs))
        holder.build()

    holder = BuilderHolder('master')
    #holder.push(ItemFeatureBuilder(master_db))
    #holder.push(PopularityCalc(master_db, lang_dbs))
    holder.push(FeatureRelationBuilder(master_db, lang_dbs))
    holder.build()
