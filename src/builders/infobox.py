import json
from circus_itertools import lazy_chunked as chunked
from .builder_utils import find_links_from_wiki, PageNameResolver


class Builder:
    def __init__(self, wiki_db):
        self.db = wiki_db

    def build(self):
        for records in chunked(self.generate_infobox_records(), 100):
            self.db.multiInsert(
                'an_infobox',
                ['page_id', 'infotype', 'key_lower', 'page_id_to'],
                [[
                    r['page_id'],
                    r['infotype'],
                    r['key_lower'],
                    r['page_id_to'],
                ] for r in records])
        self.db.commit()

    def generate_infobox_records(self):
        resolver = PageNameResolver(self.db)
        for r in self.db.generate_records(
                'an_page',
                ['page_id', 'infotype', 'infocontent'],
                cond='infotype is not NULL'):
            content_dict = json.loads(r['infocontent'])

            invalid = False
            records = []
            for key, content in content_dict.items():
                if len(key) > 100:
                    print('[TODO]Too long key: ', key, r['page_id'], r['infotype'])
                    invalid = True
                    break
                elif len(key) == 0:
                    print('[TODO]Empty key: ', key, r['page_id'], r['infotype'])
                    invalid = True
                    break
                links = find_links_from_wiki(content)
                for page_dict in [resolver.get_dict(link) for link in links]:
                    if page_dict is not None:
                        records.append({
                            'page_id': r['page_id'],
                            'infotype': r['infotype'],
                            'key_lower': key.lower(),
                            'page_id_to': page_dict['page_id'],
                            })
            if not invalid:
                for record in records:
                    yield record
