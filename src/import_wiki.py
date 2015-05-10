import argparse, sys, os, os.path, subprocess, shlex
import MySQLdb
import MySQLdb.cursors
from dbutils import TableIndexHolder

def open_db(db):
    return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db=db, charset='utf8')

def open_cur(conn):
    return conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

def command(cmd, output=True):
    print(cmd)
    if output:
        p = subprocess.Popen(cmd, shell=True, stdout=sys.stdout, stderr=sys.stderr) 
        p.wait()
        return
    else:
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        p.wait()
        stdout_data, stderr_data = p.communicate()
        return stdout_data.decode('utf-8')

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
        elif file.endswith('pages-articles.xml.bz2'):
            rs['page'] = file
    if len(rs) == 5:
        return { key: os.path.join(path, f) for key, f in rs.items()}
    else:
        return None

def init_database(database, schema_file):
    if not os.path.exists(schema_file):
        raise Exception('Schema does not exist: %s', schema_file)

    conn = open_db('')
    cur = open_cur(conn)
    cur.execute('SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s', (database,))
    if len(cur.fetchall()) != 0:
        cur.close()
        conn.close()
        raise Exception('%s has already exists' % (database,))
    cur.close()

    conn.query('create database %s' % (database, ))
    command('mysql -u root -h 127.0.0.1 %s < %s' % (database, schema_file))
    conn.select_db(database)
    conn.query("""
        alter table revision modify rev_comment MEDIUMBLOB
    """)
    conn.query("""
        alter table page modify page_title VARBINARY(255)
    """)
    conn.close()
    return


def import_from_sql(database, table, absolute_path, work_dir):
    def open_db_with_name():
        return open_db(database)

    os.chdir(work_dir)
    rt = command('ls', output=False)
    if rt:
        raise Exception('work_space is not empty')
        
    holder = TableIndexHolder.open(open_db_with_name, table)
    command('gunzip -c %s > tmp.sql' % (absolute_path, )) 
    command('split -b 50m tmp.sql _')
    rt = command('ls _*', output=False)
    rt = rt.split('\n')
    first_chunk = rt[0]
    with open(first_chunk, 'r+b') as f:
        lines  = f.readlines()
        in_create_context = False
        for i, line in enumerate(lines):
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
    command('ls _* | xargs cat > tmp.sql')
    command('rm -f _*')
    command('mysql -u root -h 127.0.0.1 %s < tmp.sql' % (database, ))
    command('rm -f tmp.sql')
    holder.close()

def import_pages(mysql_connector_jar, mwdumper_jar, database, dump_file):
    def open_db_with_name():
        return open_db(database)

    tables = ['page', 'revision', 'text']
    handlers = [TableIndexHolder.open(open_db_with_name, table) for table in tables]
    
    command("""
        java -server -classpath %s:%s \
        org.mediawiki.dumper.Dumper --output=mysql://127.0.0.1/%s?user=root  \
        --format=sql:1.5 %s
    """ % (mysql_connector_jar, mwdumper_jar, database, dump_file) )

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
    dump_dir = os.path.join(current_dir, args['path'])
    work_dir = os.path.join(current_dir, args['work_dir'])
    mysql_jar = os.path.join(current_dir, args['mysql_jar'])
    wiki_jar = os.path.join(current_dir, args['wiki_jar'])

    files = find_dumps(dump_dir)
    if not files:
        raise Exception('Not found dump files in %s.', dump_dir)

    init_database(args['db'], args['schema'])
    import_pages(mysql_jar, wiki_jar, args['db'],  files['page'])

    import_from_sql(args['db'], 'category', files['category'], work_dir)
    import_from_sql(args['db'], 'categorylinks', files['categorylinks'], work_dir)
    import_from_sql(args['db'], 'pagelinks', files['pagelinks'], work_dir)
    import_from_sql(args['db'], 'langlinks', files['langlinks'], work_dir)

    print('Finished')

    

