import argparse, gzip, os, re, urllib.parse
from dbutils import WikiDB
from fileutils import find, Workspace
from processutils import command
from circus_itertools import lazy_chunked as chunked

p = re.compile('([\d]{4})-([\d]{2})')

def get_year_from_path(gz_file):
    year = None
    for part in gz_file.split('/'):
        m = p.search(part)
        if m is not None:
            year = m.group(1)
            break

    if year is None:
        raise Exception('Does not find year from', gz_file)

    return year
        

def get_content(gz_file):
    content = None
    with gzip.open(gz_file) as f:
        content = f.read().decode('utf-8')

    return content

def info_generator_from_data_dir(data_dir):
    gz_files = find('*.gz', data_dir)
    print('Find %s gz files' % (len(gz_files),))

    for gz_file in gz_files:
        print('File: ', gz_file)
        year = get_year_from_path(gz_file)
        content = get_content(gz_file)

        lines = content.split('\n')
        for line in lines:
            parts = line.split(' ')
            if len(parts) != 4:
                raise Exception('Invalid line: ', line)

            lang = parts[0]
            path = parts[1]
            count = parts[2]
            yield {'year': year, 'lang': lang, 'path': path, 'count': count}

def create_lang_to_data(langs):
    lang_to_data = {}
    for lang in langs:
        db = WikiDB(lang)
        cat_to_id = {}
        for data in db.allCategoryDataGenerator(dict_format=True):
            cat_to_id[data['title']] = data['cat_id']

        page_to_id = {}
        for data in db.allFeaturedPageGenerator(dictFormat=True, featured=False):
            page_to_id[data['name']] = data['page_id']

        lang_to_data[lang] = { \
            'db': db, \
            'page_to_id': page_to_id, \
            'cat_to_id': cat_to_id, \
            }
    return lang_to_data

def insert_page(db, insert_buffer):
    db.multiInsert('an_pagecount', \
        ['page_id', 'year', 'count'], \
        [ [r['page_id'], r['year'], r['count']] for r in insert_buffer], \
        'count = values(count) + count')
    db.commit()

def insert_cat(db, insert_buffer):
    db.multiInsert('an_catcount', \
        ['cat_id', 'year', 'count'], \
        [ [r['cat_id'], r['year'], r['count']] for r in insert_buffer], \
        'count = values(count) + count')
    db.commit()

class CountHolder:
    def __init__(self):
        """
        Means (content_id, year) => sum(count)
        """
        self.counts = {} 

    def add(self, content_id, year, count):
        count = int(count)
        key = (content_id, year)
        if key in self.counts:
            self.counts[key] += count
        else :
            self.counts[key] = count
    
    def generate_record(self):
        for key in self.counts:
            data = self.counts[key]
            content_id = key[0]
            year = key[1]
            count = self.counts[key]
            yield [content_id, year, count]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path')
    parser.add_argument('-l', '--langs') # ja,en
    args = parser.parse_args()
    args = vars(args)

    langs = args['langs'].split(',')
    current_dir = os.getcwd()
    data_dir = os.path.join(current_dir, args['path'])

    lang_to_data = create_lang_to_data(langs)

    infos = info_generator_from_data_dir(data_dir)
    infos = filter(lambda n: n['lang'] in langs, infos)

    lang_to_page_count = { lang: CountHolder() for lang in langs }
    lang_to_cat_count = { lang: CountHolder() for lang in langs }
    all_count = 0
    hit_count = 0
    for info in infos:
        lang = info['lang']
        path = info['path']
        count = info['count']
        year = info['year']
        
        data = lang_to_data[lang]

        db = data['db']
        cat_to_id = data['cat_to_id']
        page_to_id = data['page_to_id']
        if path.lower().startswith('category:'):
            cat_name = path[len('category:'):]
            cat_id = None
            if cat_name in cat_to_id:
                cat_id = cat_to_id[cat_name]
            else:
                unquoted_name = urllib.parse.unquote(cat_name)
                if unquoted_name in cat_to_id:
                    cat_id = cat_to_id[unquoted_name]

            if cat_id is not None:
                lang_to_cat_count[lang].add(cat_id, year, count)
                hit_count += 1
        
        else:
            page_id = None
            if path in page_to_id:
                page_id = page_to_id[path]
            else:
                unquoted_name = urllib.parse.unquote(path)
                if unquoted_name in page_to_id:
                    page_id = page_to_id[unquoted_name]

            if page_id is not None:
                lang_to_page_count[lang].add(page_id, year, count)
                hit_count += 1

        all_count += 1
        if all_count % 100000 == 0:
            print('current: %s, rate: %s' % (all_count, hit_count / all_count))

    for lang in langs:
        db = lang_to_data[lang]['db']
        for records in chunked(lang_to_cat_count[lang].generate_record(), 1000):
            db.multiInsert('an_catcount', \
                ['cat_id', 'year', 'count'], \
                records, \
                'count = values(count) + count')
        db.commit()

        for records in chunked(lang_to_page_count[lang].generate_record(), 1000):
            db.multiInsert('an_pagecount', \
                ['page_id', 'year', 'count'], \
                records, \
                'count = values(count) + count')
        db.commit()

