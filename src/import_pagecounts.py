import argparse, gzip, os, re, urllib.parse
from dbutils import WikiDB
from fileutils import find, Workspace
from processutils import command

p = re.compile('([\d]{4})-([\d]{2})')

def get_year_and_content(gz_file):
    dirs = gz_file.split('/')
    year = None
    for part in gz_file.split('/'):
        m = p.search(part)
        if m is not None:
            year = m.group(1)
            break

    if year is None:
        raise Exception('Does not find year from', gz_file)
        
    content = None
    with gzip.open(gz_file) as f:
        content = f.read().decode('utf-8')

    return (year, content)

def info_generator_from_data_dir(data_dir):
    gz_files = find('*.gz', data_dir)
    print('Find %s gz files' % (len(gz_files),))

    for gz_file in gz_files:
        print('File: ', gz_file)
        year, content = get_year_and_content(gz_file)

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

    insert_cat_buffer = { lang: [] for lang in langs }
    insert_page_buffer = { lang: [] for lang in langs }
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
                insert_cat_buffer[lang].append({'cat_id': cat_id, 'year': year, 'count': count})
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
                insert_page_buffer[lang].append({'page_id': page_id, 'year': year, 'count': count})
                hit_count += 1

        if len(insert_cat_buffer[lang]) >= 100:
            insert_cat(db, insert_cat_buffer[lang])
            insert_cat_buffer[lang] = []

        if len(insert_page_buffer[lang]) >= 100:
            insert_page(db, insert_page_buffer[lang])
            insert_page_buffer[lang] = []

        all_count += 1
        if all_count % 100000 == 0:
            print('current: %s, rate: %s' % (all_count, hit_count / all_count))

    for lang in langs:
        db = lang_to_data[lang]['db']
        if len(insert_cat_buffer[lang]) > 0:
            insert_cat(db, insert_cat_buffer[lang])

        if len(insert_page_buffer[lang]) > 0:
            insert_page(db, insert_page_buffer[lang])







                
                
