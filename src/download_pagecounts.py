import re, argparse, os, gzip, urllib
import http.client
from dbutils import *
from circus_itertools import lazy_chunked as chunked
from fileutils import save_content


def get_content(path):
    conn = http.client.HTTPConnection('dumps.wikimedia.org')
    print('get:', path)
    conn.request('GET', path)
    res = conn.getresponse()
    print(res.status)
    if res.status != 200:
        res.close()
        conn.close()
        return None
    data = res.read()
    res.close()
    conn.close()

    return data

#def has_content(year, month, dump_dir, file_name):
#    dest_dir = os.path.join(dump_dir, str(year), '%02d' % (month,))
#    dest_file = os.path.join(dest_dir, file_name)
#    exists = os.path.exists(dest_file)
#    if exists:
#        print('already exists: %s' % (dest_file,))
#    return exists

years = range(2007, 2015 + 1)
months = range(1, 12 + 1)

def pagecount_gz_generator(interval=1):
    p = re.compile('<a href="(pagecounts-[\d]+-[\d]+\.gz)">')
    cnt = 0
    for year in years:
        for month in months:
            index_page = '/other/pagecounts-raw/%s/%s-%02d/' % (year, year, month)
            data = get_content(index_page)
            if data is None:
                continue
            data = data.decode('utf-8')

            while 1:
                m = p.search(data)
                if m is None:
                    break
                file_name = m.group(1)
                if cnt % interval == 0:
                    path = index_page + file_name
                    yield {'year':year, 'path':path, 'gz':get_content(path)}
                cnt += 1
                data = data[m.end():]

def record_generator(langs, content_iter):
    for data in content_iter:
        year = data['year']
        binary = data['bin']
        for line in binary.splitlines():
            try:
                line = line.decode('utf-8')
            except:
                continue

            cols = line.split(' ')
            if len(cols) != 4:
                print('Invalid line: ', line)
                continue

            project = cols[0].lower()
            name = urllib.parse.unquote(cols[1])
            count = cols[2]

            if project in langs:
                if len(name) > 255:
                    continue
                yield {'lang': project, 'year': year, 'name': name, 'count': count, 'path': data['path'], 'line': line}

def insert_pagecount(langs, record_iter):
    lang_to_db = {}
    for lang in langs:
        lang_to_db[lang] = WikiDB(lang)

    for records in chunked(record_iter, 100):
        lang_to_records = { lang:[] for lang in langs }
        for record in records:
            lang_to_records[record['lang']].append(record)

        for lang, records in lang_to_records.items():
            if len(records) == 0:
                continue
            db = lang_to_db[lang]
            try:
                db.multiInsert('an_pagecount', \
                    ['name', 'year', 'count'], \
                    [ [r['name'], r['year'], r['count']] for r in records], \
                    'count = values(count) + count')
            except Exception as e:
                print('Fail to insert', e)

            db.commit()

def save_to_local(dump_dir, langs, record_iter):
    lang_to_db = {}
    lang_to_page_dict = {}
    lang_to_category_dict = {}
    for lang in langs:
        db = WikiDB(lang)
        lang_to_db[lang] = db
        category_dict = {}
        for data in db.allCategoryDataGenerator(dict_format=True):
            category_dict[data['title']] = None

        page_dict = {}
        for title in db.allPageTitlesGenerator():
            page_dict[title] = None

    last_path = None
    lines = []
    for record in record_iter:
        if last_path is not None and last_path != record['path']:
            save_content(os.path.join(dump_dir, last_path[1:]), '\n'.join(lines))
            lines = []

        lines.append(record['line'])
        last_path = record['path']
        pass

    save_content(os.path.join(dump_dir, last_path[1:]), '\n'.join(lines))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path')
    args = parser.parse_args()
    args = vars(args)
    current_dir = os.getcwd()
    dump_dir = os.path.join(current_dir, args['path'])

    langs = ['en', 'ja']
    gz_iter = pagecount_gz_generator(100)
    content_iter = map(lambda x: {'path': x['path'], 'year': x['year'], 'bin': gzip.decompress(x['gz'])}, gz_iter) 
    record_iter = record_generator(langs, content_iter)
    #for ret in insert_pagecount(langs, record_iter):
    #    pass

    for ret in save_to_local(dump_dir, langs, record_iter):
        pass
        
        
        


