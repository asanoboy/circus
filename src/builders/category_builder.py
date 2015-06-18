from consts import valid_infotypes
from models import createPageInfoByBracketText, Category
from parser import getBracketTexts


class CategoryBuilder:
    def __init__(self, wiki_db):
        self.wiki_db

    def build(self):
        buildCatInfo(self.wiki_db)


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


def buildCatInfo(wiki_db):

    catDataIter = wiki_db.allCategoryDataGenerator()
    catIter = filter(lambda x: x, map(createCategoryWithoutStub, catDataIter))
    for cat in catIter:
        records = wiki_db.selectAndFetchAll("""
            select px.infotype, count(*) page_num from categorylinks cl
            inner join an_page px on px.page_id = cl.cl_from
            where cl.cl_type = 'page' and cl.cl_to = %s
            group by px.infotype
            order by px.page_id asc
            """, (cat.name, ))
        valuesList = [
            [cat.id, record['infotype'], record['page_num']]
            for record in records]
        if len(valuesList) > 0:
            wiki_db.multiInsert(
                'an_category_info',
                ['cat_id', 'infotype', 'page_num'],
                valuesList)

    wiki_db.commit()


def updateFeatured(wiki_db):
    infotypes = valid_infotypes(wiki_db.lang)
    wiki_db.updateQuery("""
        update an_info
        set featured = case when %s then 1 else 0 end
        """ % (' or '.join(['name = %s'] * len(infotypes)), ),
        set(infotypes,))
    wiki_db.commit()

    wiki_db.updateQuery("""
        update an_category_info
        set featured = case when %s then 1 else 0 end
        """ % (' or '.join(['infotype = %s'] * len(infotypes)), ),
        set(infotypes,))
    wiki_db.commit()
