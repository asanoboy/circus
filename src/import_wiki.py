import argparse
import sys
import os
import os.path
import MySQLdb
import MySQLdb.cursors
from dbutils import TableIndexHolder
from processutils import command
from fileutils import Workspace


def open_db(db):
    return MySQLdb.connect(
        host="127.0.0.1", user="root", passwd="", db=db, charset='utf8')


def open_cur(conn):
    return conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)


def find_dumps(path):
    files = os.listdir(path)
    rs = {}
    for file in files:
        if file.endswith('pagelinks.sql.gz'):
            rs['pagelinks'] = file
        elif file.endswith('category.sql.gz'):
            rs['category'] = file
        elif file.endswith('categorylinks.sql.gz'):
            rs['categorylinks'] = file
        elif file.endswith('langlinks.sql.gz'):
            rs['langlinks'] = file
        elif file.endswith('image.sql.gz'):
            rs['image'] = file
        elif file.endswith('imagelinks.sql.gz'):
            rs['imagelinks'] = file
        elif file.endswith('redirect.sql.gz'):
            rs['redirect'] = file
        elif file.endswith('pages-articles.xml.bz2'):
            rs['page'] = file
    if len(rs) == 8:
        return {key: os.path.join(path, f) for key, f in rs.items()}
    else:
        return None


def init_database(database, schema_file):
    if not os.path.exists(schema_file):
        raise Exception('Schema does not exist: %s', schema_file)

    conn = open_db('')
    cur = open_cur(conn)
    cur.execute('''
        SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME = %s
        ''', (database,))
    if len(cur.fetchall()) != 0:
        cur.close()
        conn.close()
        raise Exception('%s has already exists' % (database,))
    cur.close()

    conn.query('create database %s' % (database, ))
    command('mysql -u root -h 127.0.0.1 %s < %s' % (database, schema_file))
    conn.select_db(database)
    conn.query('alter table revision modify rev_comment MEDIUMBLOB')
    conn.query('alter table revision modify rev_user_text VARBINARY(255)')
    conn.query('alter table page modify page_title VARBINARY(255)')

    cur = open_cur(conn)
    cur.execute('show table status')
    for status in cur.fetchall():
        if status['Name'] not in ['page', 'revision', 'text']:
            conn.query('drop table %s' % (status['Name'],))
    pass
    cur.close()
    conn.close()
    return


def import_from_sql(database, table, absolute_path, no_index=False):
    def open_db_with_name():
        return open_db(database)

    rt = command('ls', output=False)
    if rt:
        raise Exception('work_space is not empty')

    command('gunzip -c %s > tmp.sql' % (absolute_path, ))
    command('split -b 50m tmp.sql _')
    rt = command('ls _*', output=False)
    rt = rt.split('\n')
    first_chunk = rt[0]
    schema_lines = []
    with open(first_chunk, 'r+b') as f:
        lines = f.readlines()
        in_create_context = False
        for i, line in enumerate(lines):
            schema_lines.append(line)
            line = line.decode('utf-8')
            if line.lower().startswith('drop table'):
                lines[i] = b''
            elif line.lower().startswith('create'):
                in_create_context = True

            if in_create_context:
                lines[i] = b''
                if line.startswith(')'):
                    break
        f.seek(0)
        f.truncate()
        f.write(b''.join(lines))

    with open('create.sql', 'w+b') as f:
        f.write(b''.join(schema_lines))

    command('ls _* | xargs cat > tmp.sql')
    command('rm -f _*')
    command('mysql -u root -h 127.0.0.1 %s < create.sql' % (database, ))
    command('rm -f create.sql')
    with TableIndexHolder(open_db_with_name, table, no_index=no_index):
        command('mysql -u root -h 127.0.0.1 %s < tmp.sql' % (database, ))
        command('rm -f tmp.sql')


def import_pages(mysql_connector_jar, mwdumper_jar, database, dump_file):
    def open_db_with_name():
        return open_db(database)

    tables = ['page', 'revision', 'text']
    handlers = [
        TableIndexHolder.open(open_db_with_name, table)
        for table in tables]

    command("""
        java -server -classpath %s:%s \
        org.mediawiki.dumper.Dumper --output=mysql://127.0.0.1/%s?user=root  \
        --format=sql:1.5 %s
    """ % (mysql_connector_jar, mwdumper_jar, database, dump_file))

    for h in handlers:
        h.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path')
    parser.add_argument('-w', '--work_dir')
    parser.add_argument('-d', '--db')
    parser.add_argument('-s', '--schema')
    parser.add_argument('-m', '--mysql_jar')
    parser.add_argument('-j', '--wiki_jar')
    args = parser.parse_args()
    args = vars(args)
    current_dir = os.getcwd()
    schema = os.path.join(current_dir, args['schema'])
    dump_dir = os.path.join(current_dir, args['path'])
    work_dir = os.path.join(current_dir, args['work_dir'])
    mysql_jar = os.path.join(current_dir, args['mysql_jar'])
    wiki_jar = os.path.join(current_dir, args['wiki_jar'])

    files = find_dumps(dump_dir)
    if not files:
        raise Exception('Not found dump files in %s.', dump_dir)

    with Workspace(work_dir):
        # import_pages(mysql_jar, wiki_jar, args['db'],  files['page'])
        # import_from_sql(args['db'], 'category', files['category'])
        # import_from_sql(args['db'], 'langlinks', files['langlinks'])
        import_from_sql(args['db'], 'redirect', files['redirect'])
        sys.exit()
        import_from_sql(
            args['db'], 'imagelinks', files['imagelinks'])
        import_from_sql(
            args['db'], 'image', files['image'], work_dir)
        import_from_sql(
            args['db'], 'categorylinks', files['categorylinks'],
            no_index=True)
        import_from_sql(
            args['db'], 'pagelinks', files['pagelinks'], no_index=True)

    conn = open_db(args['db'])
    """
    Modified large index to save storage.
    """
    conn.query('alter table categorylinks add index cl_from(cl_from)')
    conn.query('alter table categorylinks add index cl_to(cl_to, cl_type)')
    conn.query('alter table pagelinks add index pl_from(pl_from)')
    conn.query('alter table pagelinks add index pl_to(pl_namespace, pl_title)')

    conn.close()

    print('Finished')
