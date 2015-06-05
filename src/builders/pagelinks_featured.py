from models import Page
from circus_itertools import lazy_chunked as chunked
from dbutils import selectGenerator

def replace(string, orig, dest):
    return dest.join(string.split(orig))

class PagelinksFeaturedBuilder:
    def __init__(self, wiki_db):
        self.db = wiki_db

    def _generate_musician_pagelinks(self):
        musician_info = 'Infobox_musical_artist' if self.db.lang == 'en' \
            else 'Infobox_Musician'

        last_page_id_from = None
        feature_page_datas = []
        for record in selectGenerator(self.db.openConn, \
               'an_pagelinks_filtered pl', \
                cols=['pl.id_from', 'pl.id_to', 'pl.pos_info', 'pto.infotype'], \
                joins=[\
                    'inner join an_page pfrom on pfrom.page_id = pl.id_from', \
                    'inner join an_page pto   on pto.page_id   = pl.id_to', \
                ], \
                cond='pfrom.infotype = %s and pl.in_infobox = 1', \
                order='pl.id_from asc', \
                arg=(musician_info,) \
                ):
            page_id_from = record[0]
            if last_page_id_from is not None and last_page_id_from != page_id_from:
                yield (last_page_id_from, feature_page_datas)
                feature_page_datas = []

            feature_page_datas.append({'page_id': record[1], 'pos_info': record[2], 'infotype': record[3]})
            last_page_id_from = page_id_from

        if len(feature_page_datas) > 0:
            yield (last_page_id_from, feature_page_datas)

    def _generate_pagelinks(self):
        for id_from, feature_page_datas in self._generate_musician_pagelinks():
            infotype_to_datas = {}
            for data in feature_page_datas:
                infotype = data['infotype']
                if infotype not in infotype_to_datas:
                    infotype_to_datas[infotype] = []
                #infotype_to_datas[infotype].append(data['page_id'], data['pos_info']))
                infotype_to_datas[infotype].append(data)

            for infotype, datas in infotype_to_datas.items():
                length = len(datas)
                datas = sorted(datas, key=lambda x: int(x['pos_info']))
                sum_strength = 0
                res_records = []
                for i, data in enumerate(datas):
                    strength = 1 / (1+i) # TODO: arrangement
                    res_records.append( [id_from, data['page_id'], strength] )
                    sum_strength += strength

                for record in res_records:
                    record[2] /= sum_strength
                    yield( record )

    def build(self):
        for records in chunked(self._generate_pagelinks(), 10000):
            self.db.multiInsert('an_pagelinks_featured', \
                ['id_from', 'id_to', 'strength'], \
                records, \
                'strength = values(strength) + strength')
        
        self.db.commit()


