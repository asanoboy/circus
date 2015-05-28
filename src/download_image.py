import os, hashlib, argparse, http.client
from dbutils import WikiDB
from fileutils import save_content

def get_image_path(filename, lang='commons'):
    digest = hashlib.md5(filename.encode('utf-8')).hexdigest()
    path = '/wikipedia/%s/%s/%s/%s' % (lang, digest[0], digest[0:2], filename)
    return path

def get_content(path):
    conn = http.client.HTTPConnection('upload.wikimedia.org')
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path')
    parser.add_argument('-l', '--lang')
    args = parser.parse_args()
    args = vars(args)

    lang = args['lang']
    current_dir = os.getcwd()
    dest_dir = os.path.join(current_dir, args['path'])
    
    db = WikiDB(lang)
    pages = db.allFeaturedPageGenerator(dictFormat=True, featured=False)
    for page in pages:
        records = db.selectAndFetchAll("""
        select il.il_to, p.page_id from imagelinks il
        left join page p on p.page_title = il.il_to and p.page_namespace = 6
        where il.il_from = %s
        """, (page['page_id'],), dictFormat=True, decode=True)

        top_record = None
        top_pos = -1 
        for record in records:
            filename = record['il_to']
            pos = page['infocontent'].find(filename)
            if pos == -1:
                renamed_filename = ' '.join(filename.split('_'))
                pos = page['infocontent'].find(renamed_filename)

            if (top_pos >= 0 and pos >= 0 and top_pos > pos) \
                    or (top_pos == -1 and pos >= 0):
                top_pos = pos
                top_record = record

        if top_record is None:
            continue

        print('has top_record', page['page_id'], top_record)

        """ wiki spec """
        path = get_image_path(top_record['il_to']) \
            if top_record['page_id'] is None \
            else get_image_path(top_record['il_to'], lang)

        dest_path = os.path.join(dest_dir, path[1:])
        if not os.path.exists(dest_path):
            image = get_content(path)
            if image is None:
                print('File doesn\'t exist: ', path)
            else:
                save_content(dest_path, image)







